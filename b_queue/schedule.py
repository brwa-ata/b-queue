"""The scheduler: entries declared in code, queued when they are due.

    # schedules.py (in any installed app, or next to settings.py)
    from b_queue import schedule
    from customers.tasks import send_email, calc_balance

    schedule.task(send_email).daily_at('01:00').name('Send Email')
    schedule.task(calc_balance, 42).every_five_minutes().without_overlapping()
    schedule.command('clearsessions').weekly_on(5, '03:00')

Nothing runs inside the scheduler itself: a due entry is put on the queue and
a worker runs it. Each entry is queued once per time slot, however many
schedulers run (several servers, cron firing twice): the claim is atomic in
the queue's storage.

Times are in settings.TIME_ZONE unless the entry says `.timezone(...)`.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import datetime, time
from importlib import import_module
from importlib.util import find_spec
from zoneinfo import ZoneInfo

from django.conf import settings
from django.tasks import Task, task_backends
from django.utils import timezone
from django.utils.module_loading import autodiscover_modules

from b_queue import conf
from b_queue.context import activate
from b_queue.cron import Cron
from b_queue.exceptions import QueueError

logger = logging.getLogger('b_queue')

POSITIONS = {'minute': 0, 'hour': 1, 'day_of_month': 2, 'month': 3, 'day_of_week': 4}


def _hour_minute(text: str) -> tuple[int, int]:
    try:
        hour, minute = text.split(':')
        parsed = time(int(hour), int(minute))
    except ValueError:
        raise QueueError(f'Invalid time "{text}": expected "HH:MM"') from None
    return parsed.hour, parsed.minute


class Event:
    """One schedule entry. Every method returns the entry, so they chain."""

    def __init__(self, task: Task, args=(), kwargs=None, label: str | None = None):
        self.task = task
        self.args = tuple(args)
        self.kwargs = dict(kwargs or {})
        self.fields = ['*', '*', '*', '*', '*']
        self._default_label = label
        self._name = None
        self._overlap_expires = None  # seconds; None: overlapping allowed
        self._queue = None
        self._timezone = None
        self._filters = []
        self._for_each = None

    # -- identity ---------------------------------------------------------
    def name(self, name: str) -> Event:
        self._name = name
        return self

    @property
    def label(self) -> str:
        """Identifies the entry in the stored state; unique per schedule."""
        return self._name or self._default_label or self.task.module_path

    @property
    def expression(self) -> str:
        return ' '.join(self.fields)

    # -- frequency --------------------------------------------------------
    def cron(self, expression: str) -> Event:
        try:
            Cron(expression)
        except ValueError as error:
            raise QueueError(str(error)) from None
        self.fields = expression.split()
        return self

    def _set(self, **positions) -> Event:
        fields = list(self.fields)
        for position, value in positions.items():
            fields[POSITIONS[position]] = str(value)
        return self.cron(' '.join(fields))

    def every_minute(self) -> Event:
        return self._set(minute='*')

    def every_two_minutes(self) -> Event:
        return self._set(minute='*/2')

    def every_three_minutes(self) -> Event:
        return self._set(minute='*/3')

    def every_four_minutes(self) -> Event:
        return self._set(minute='*/4')

    def every_five_minutes(self) -> Event:
        return self._set(minute='*/5')

    def every_ten_minutes(self) -> Event:
        return self._set(minute='*/10')

    def every_fifteen_minutes(self) -> Event:
        return self._set(minute='*/15')

    def every_thirty_minutes(self) -> Event:
        return self._set(minute='*/30')

    def hourly(self) -> Event:
        return self._set(minute=0)

    def hourly_at(self, *minutes: int) -> Event:
        return self._set(minute=','.join(str(minute) for minute in minutes))

    def every_two_hours(self) -> Event:
        return self._set(minute=0, hour='*/2')

    def every_three_hours(self) -> Event:
        return self._set(minute=0, hour='*/3')

    def every_four_hours(self) -> Event:
        return self._set(minute=0, hour='*/4')

    def every_six_hours(self) -> Event:
        return self._set(minute=0, hour='*/6')

    def daily(self) -> Event:
        return self._set(minute=0, hour=0)

    def daily_at(self, at: str) -> Event:
        hour, minute = _hour_minute(at)
        return self._set(minute=minute, hour=hour)

    at = daily_at

    def twice_daily(self, first: int = 1, second: int = 13) -> Event:
        return self._set(minute=0, hour=f'{first},{second}')

    def weekly(self) -> Event:
        return self._set(minute=0, hour=0, day_of_week=0)

    def weekly_on(self, weekday: int, at: str = '00:00') -> Event:
        """`weekday` as in cron: 0 Sunday, 1 Monday ... 6 Saturday."""
        hour, minute = _hour_minute(at)
        return self._set(minute=minute, hour=hour, day_of_week=weekday)

    def monthly(self) -> Event:
        return self._set(minute=0, hour=0, day_of_month=1)

    def monthly_on(self, day: int = 1, at: str = '00:00') -> Event:
        hour, minute = _hour_minute(at)
        return self._set(minute=minute, hour=hour, day_of_month=day)

    def quarterly(self) -> Event:
        return self._set(minute=0, hour=0, day_of_month=1, month='1-12/3')

    def yearly(self) -> Event:
        return self._set(minute=0, hour=0, day_of_month=1, month=1)

    def yearly_on(self, month: int = 1, day: int = 1, at: str = '00:00') -> Event:
        hour, minute = _hour_minute(at)
        return self._set(minute=minute, hour=hour, day_of_month=day, month=month)

    # -- day constraints (combine with a frequency) ----------------------
    def days(self, *weekdays: int) -> Event:
        return self._set(day_of_week=','.join(str(day) for day in weekdays))

    def weekdays(self) -> Event:
        return self._set(day_of_week='1-5')

    def weekends(self) -> Event:
        return self._set(day_of_week='0,6')

    def sundays(self) -> Event:
        return self.days(0)

    def mondays(self) -> Event:
        return self.days(1)

    def tuesdays(self) -> Event:
        return self.days(2)

    def wednesdays(self) -> Event:
        return self.days(3)

    def thursdays(self) -> Event:
        return self.days(4)

    def fridays(self) -> Event:
        return self.days(5)

    def saturdays(self) -> Event:
        return self.days(6)

    # -- other constraints ------------------------------------------------
    def between(self, start: str, end: str) -> Event:
        """Only between two times of day: `.hourly().between('8:00', '17:00')`.
        A window may wrap midnight ('22:00', '02:00')."""
        window = time(*_hour_minute(start)), time(*_hour_minute(end))
        self._filters.append(lambda now: _in_window(now.time(), *window))
        return self

    def unless_between(self, start: str, end: str) -> Event:
        window = time(*_hour_minute(start)), time(*_hour_minute(end))
        self._filters.append(lambda now: not _in_window(now.time(), *window))
        return self

    def when(self, condition) -> Event:
        """Run only if `condition()` is true at that moment."""
        self._filters.append(lambda now: bool(condition()))
        return self

    def skip(self, condition) -> Event:
        self._filters.append(lambda now: not condition())
        return self

    def timezone(self, name: str) -> Event:
        self._timezone = ZoneInfo(name)
        return self

    # -- options ----------------------------------------------------------
    def without_overlapping(self, expires_after: int = 1440) -> Event:
        """Skip a slot while the run of an earlier slot is unfinished (still
        queued, running or waiting for a retry). For entries that run often
        and can take longer than their interval. The block gives up after
        `expires_after` minutes, so one stuck run cannot stop the entry
        forever."""
        self._overlap_expires = expires_after * 60
        return self

    def on_queue(self, queue_name: str) -> Event:
        self._queue = queue_name
        return self

    def for_each(self, contexts) -> Event:
        """Multi-tenancy: queue one job per context, e.g. per active tenant.
        `contexts` is a list of context dicts, or a function returning one
        (called every time the entry is due):

            schedule.task(stock_backup).daily_at('01:00').for_each(
                lambda: [{'tenant': id} for id in active_tenant_ids()]
            )

        Needs a context provider (B_QUEUE['CONTEXT']). Without for_each() a
        scheduled job carries whatever the provider captures in the
        scheduler process, normally nothing."""
        self._for_each = contexts
        return self

    # -- running ----------------------------------------------------------
    def _local(self, now: datetime) -> datetime:
        """`now` as wall-clock time in the entry's timezone."""
        if timezone.is_naive(now):
            return now
        zone = self._timezone or timezone.get_current_timezone()
        return now.astimezone(zone)

    def due_at(self, now: datetime) -> datetime:
        """The latest slot of this entry that is not after `now`."""
        local = self._local(now)
        slot = Cron(self.expression).previous(local.replace(tzinfo=None))
        return slot.replace(tzinfo=local.tzinfo)

    def next_due(self, now: datetime) -> datetime:
        local = self._local(now)
        slot = Cron(self.expression).next(local.replace(tzinfo=None))
        return slot.replace(tzinfo=local.tzinfo)

    def passes_filters(self, now: datetime) -> bool:
        local = self._local(now)
        return all(check(local) for check in self._filters)

    def dispatch(self) -> list:
        """Queue the entry. Returns the task results that were queued: one,
        or one per context with for_each(); empty when without_overlapping()
        held everything back."""
        if self._for_each is None:
            return self._dispatch_one(None)

        source = self._for_each
        results = []
        for context in source() if callable(source) else source:
            try:
                results += self._dispatch_one(context)
            except Exception:
                # one broken tenant must not hold back the others
                logger.exception(
                    'Could not queue schedule entry %s for %s', self.label, context
                )
        return results

    def _dispatch_one(self, context) -> list:
        from b_queue.backends.base import QueueBackend

        task = self.task
        if self._queue:
            task = task.using(queue_name=self._queue)

        backend = task.get_backend()
        if not isinstance(backend, QueueBackend):
            # e.g. Django's ImmediateBackend in tests: runs on the spot
            with activate(context):
                return [task.enqueue(*self.args, **self.kwargs)]

        if context is not None:
            task = replace(task, context=context)
        if self._overlap_expires is not None:
            # the context is part of the key: checked per tenant
            task = replace(
                task,
                unique=['schedule', self.label],
                unique_until='finish',
                unique_for=self._overlap_expires,
            )
        result, queued = backend.enqueue_job(task, self.args, self.kwargs)
        return [result] if queued else []


def _in_window(moment: time, start: time, end: time) -> bool:
    if start <= end:
        return start <= moment <= end
    return moment >= start or moment <= end


class _MemoryState:
    """Slot bookkeeping when the schedule's backend is not a b_queue one
    (e.g. Django's ImmediateBackend in tests): this process only."""

    def __init__(self):
        self.slots = {}

    def schedule_claim(self, name, due, now) -> bool:
        if name in self.slots and self.slots[name] >= due:
            return False
        self.slots[name] = due
        return True

    def schedule_forget(self, keep) -> None:
        for name in set(self.slots) - set(keep):
            del self.slots[name]


class Schedule:
    def __init__(self):
        self.events: list[Event] = []
        self._loaded = False
        self._memory = _MemoryState()

    # -- declaring --------------------------------------------------------
    def task(self, task: Task, *args, **kwargs) -> Event:
        """Schedule a task; extra arguments are passed to it on every run."""
        if not isinstance(task, Task):
            raise QueueError(f'schedule.task() needs a @task function, got {task!r}')
        return self._add(Event(task, args, kwargs))

    def command(self, name: str, *args, **options) -> Event:
        """Schedule a management command."""
        from b_queue.tasks import run_command

        label = ' '.join(['command:' + name, *(str(arg) for arg in args)])
        return self._add(Event(run_command, (name, *args), options, label))

    def _add(self, event: Event) -> Event:
        self.events.append(event)
        return event

    def clear(self) -> None:
        self.events.clear()

    # -- running ----------------------------------------------------------
    def load(self) -> Schedule:
        """Import the modules that declare entries (once)."""
        if not self._loaded:
            self._loaded = True
            modules = conf.get('SCHEDULE_MODULES')
            if modules is None:
                autodiscover_modules('schedules')
                project = settings.ROOT_URLCONF.rpartition('.')[0]
                modules = [f'{project}.schedules'] if project else []
                modules = [name for name in modules if _exists(name)]
            for name in modules:
                import_module(name)

        labels = [event.label for event in self.events]
        duplicates = sorted({label for label in labels if labels.count(label) > 1})
        if duplicates:
            raise QueueError(
                'Schedule entries need unique names; add .name(...) to tell '
                f'these apart: {duplicates}'
            )
        return self

    def _state(self):
        from b_queue.backends.base import QueueBackend

        backend = task_backends[conf.get('SCHEDULE_BACKEND')]
        return backend if isinstance(backend, QueueBackend) else self._memory

    def run(self, now: datetime | None = None) -> list[tuple[Event, datetime]]:
        """Queue every entry whose current slot was not queued yet. Returns
        the (entry, slot) pairs that were queued."""
        now = now or timezone.now()
        state = self._state()
        grace = conf.get('SCHEDULE_GRACE')
        ran = []

        for event in self.events:
            due = event.due_at(now)
            if not state.schedule_claim(event.label, due, now):
                continue  # this slot was handled, here or on another server
            # A slot found long after its time is skipped, not run late. That
            # covers downtime, and an entry seen for the first time: adding a
            # 01:00 entry at noon must not run it right away.
            if (now - due).total_seconds() > grace:
                continue
            if not event.passes_filters(now):
                continue
            try:
                queued = bool(event.dispatch())
            except Exception:
                # one broken entry must not hold back the others
                logger.exception('Could not queue schedule entry %s', event.label)
                continue
            if queued:
                ran.append((event, due))

        state.schedule_forget([event.label for event in self.events])
        return ran


def _exists(module: str) -> bool:
    try:
        return find_spec(module) is not None
    except (ImportError, ValueError):
        return False


schedule = Schedule()
