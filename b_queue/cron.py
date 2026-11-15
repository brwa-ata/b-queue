"""A small cron expression matcher (five fields, no dependencies).

    minute hour day-of-month month day-of-week

Each field takes `*`, numbers, ranges (`1-5`), steps (`*/10`, `8-18/2`) and
comma lists. Day-of-week is 0-6 with Sunday 0 (7 also means Sunday). As in
cron, when both day fields are restricted a day matching either one counts.
"""

from __future__ import annotations

from datetime import datetime, timedelta

RANGES = [(0, 59), (0, 23), (1, 31), (1, 12), (0, 7)]
NAMES = ['minute', 'hour', 'day of month', 'month', 'day of week']
MINUTE = timedelta(minutes=1)


def _parse_field(text: str, low: int, high: int, name: str) -> frozenset[int]:
    values = set()
    for part in text.split(','):
        span, _, step = part.partition('/')
        try:
            step = int(step) if step else 1
            if span == '*':
                start, end = low, high
            elif '-' in span:
                start, end = (int(n) for n in span.split('-'))
            else:
                start = int(span)
                # "5/10" means "from 5, every 10"
                end = high if '/' in part else start
        except ValueError:
            raise ValueError(f'Invalid cron {name} "{text}"') from None

        if step < 1 or start < low or end > high or start > end:
            raise ValueError(f'Invalid cron {name} "{text}"')
        values.update(range(start, end + 1, step))
    return frozenset(values)


class Cron:
    def __init__(self, expression: str):
        fields = expression.split()
        if len(fields) != 5:
            raise ValueError(
                f'Invalid cron expression "{expression}": expected 5 fields'
            )
        self.expression = ' '.join(fields)
        parsed = [
            _parse_field(text, low, high, name)
            for text, (low, high), name in zip(fields, RANGES, NAMES)
        ]
        self.minutes, self.hours, self.days, self.months, weekdays = parsed
        self.weekdays = frozenset(day % 7 for day in weekdays)
        self._any_day = fields[2] == '*'
        self._any_weekday = fields[4] == '*'

    def _day_matches(self, moment: datetime) -> bool:
        day = moment.day in self.days
        weekday = (moment.weekday() + 1) % 7 in self.weekdays
        if self._any_day or self._any_weekday:
            return day and weekday
        return day or weekday

    def matches(self, moment: datetime) -> bool:
        return (
            moment.minute in self.minutes
            and moment.hour in self.hours
            and moment.month in self.months
            and self._day_matches(moment)
        )

    def previous(self, moment: datetime) -> datetime:
        """The latest matching minute that is not after `moment`."""
        current = moment.replace(second=0, microsecond=0)
        limit = current.year - 8
        while current.year >= limit:
            if current.month not in self.months:
                current = current.replace(day=1, hour=0, minute=0) - MINUTE
            elif not self._day_matches(current):
                current = current.replace(hour=0, minute=0) - MINUTE
            elif current.hour not in self.hours:
                current = current.replace(minute=0) - MINUTE
            elif current.minute not in self.minutes:
                current -= MINUTE
            else:
                return current
        raise ValueError(f'Cron expression "{self.expression}" never matches')

    def next(self, moment: datetime) -> datetime:
        """The earliest matching minute after `moment`."""
        current = moment.replace(second=0, microsecond=0) + MINUTE
        limit = current.year + 8
        while current.year <= limit:
            if current.month not in self.months:
                year, month = divmod(current.month, 12)
                current = current.replace(
                    year=current.year + year, month=month + 1, day=1, hour=0, minute=0
                )
            elif not self._day_matches(current):
                current = current.replace(hour=0, minute=0) + timedelta(days=1)
            elif current.hour not in self.hours:
                current = current.replace(minute=0) + timedelta(hours=1)
            elif current.minute not in self.minutes:
                current += MINUTE
            else:
                return current
        raise ValueError(f'Cron expression "{self.expression}" never matches')
