from datetime import datetime

import pytest

from b_queue.cron import Cron


def at(text):
    return datetime.strptime(text, '%Y-%m-%d %H:%M')


@pytest.mark.parametrize(
    'expression, moment, previous, following',
    [
        ('* * * * *', '2026-03-10 10:30', '2026-03-10 10:30', '2026-03-10 10:31'),
        ('*/15 * * * *', '2026-03-10 10:31', '2026-03-10 10:30', '2026-03-10 10:45'),
        ('0 1 * * *', '2026-03-10 00:59', '2026-03-09 01:00', '2026-03-10 01:00'),
        ('0 1 * * *', '2026-03-10 01:00', '2026-03-10 01:00', '2026-03-11 01:00'),
        ('30 8 1 * *', '2026-03-10 00:00', '2026-03-01 08:30', '2026-04-01 08:30'),
        # 2026-03-10 is a Tuesday; 0 is Sunday
        ('0 0 * * 0', '2026-03-10 12:00', '2026-03-08 00:00', '2026-03-15 00:00'),
        ('0 9 * * 1-5', '2026-03-14 12:00', '2026-03-13 09:00', '2026-03-16 09:00'),
        ('0 0 1 1 *', '2026-03-10 12:00', '2026-01-01 00:00', '2027-01-01 00:00'),
        ('0 0 29 2 *', '2026-03-10 12:00', '2024-02-29 00:00', '2028-02-29 00:00'),
        ('0 8-18/5 * * *', '2026-03-10 14:00', '2026-03-10 13:00', '2026-03-10 18:00'),
        ('5,35 * * * *', '2026-12-31 23:50', '2026-12-31 23:35', '2027-01-01 00:05'),
        # 7 is Sunday too
        ('0 0 * * 7', '2026-03-10 12:00', '2026-03-08 00:00', '2026-03-15 00:00'),
    ],
)
def test_previous_and_next(expression, moment, previous, following):
    cron = Cron(expression)
    assert cron.previous(at(moment)) == at(previous)
    assert cron.next(at(moment)) == at(following)


def test_day_of_month_or_day_of_week_when_both_are_set():
    # the 13th, or any Friday (classic cron rule)
    cron = Cron('0 0 13 * 5')
    assert cron.matches(at('2026-03-13 00:00'))  # Friday the 13th
    assert cron.matches(at('2026-03-20 00:00'))  # a Friday
    assert cron.matches(at('2026-04-13 00:00'))  # a Monday the 13th
    assert not cron.matches(at('2026-03-14 00:00'))


@pytest.mark.parametrize(
    'expression',
    [
        '* * * *',
        '60 * * * *',
        '* 24 * * *',
        '* * 0 * *',
        '* * * 13 *',
        'a * * * *',
        '*/0 * * * *',
        '5-1 * * * *',
    ],
)
def test_invalid_expressions(expression):
    with pytest.raises(ValueError):
        Cron(expression)


def test_expression_that_never_matches():
    with pytest.raises(ValueError, match='never matches'):
        Cron('0 0 31 2 *').next(at('2026-01-01 00:00'))
