from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest
from django.tasks import TaskResultStatus

from b_queue import QueueError, schedule
from b_queue.schedule import Event
from tests import tasks


def utc(text):
    return datetime.strptime(text, '%Y-%m-%d %H:%M:%S').replace(tzinfo=UTC)


@pytest.fixture
def state_in(alias, settings):
    """Keep the schedule's slot bookkeeping in the backend under test."""
    settings.B_QUEUE = {'SCHEDULE_MODULES': [], 'SCHEDULE_BACKEND': alias}


def labels(ran):
    return [event.label for event, _ in ran]


# -- frequencies --------------------------------------------------------------
@pytest.mark.parametrize(
    'build, expression',
    [
        (lambda e: e.every_minute(), '* * * * *'),
        (lambda e: e.every_five_minutes(), '*/5 * * * *'),
        (lambda e: e.every_thirty_minutes(), '*/30 * * * *'),
        (lambda e: e.hourly(), '0 * * * *'),
        (lambda e: e.hourly_at(17), '17 * * * *'),
        (lambda e: e.hourly_at(15, 45), '15,45 * * * *'),
        (lambda e: e.every_two_hours(), '0 */2 * * *'),
        (lambda e: e.daily(), '0 0 * * *'),
        (lambda e: e.daily_at('01:00'), '0 1 * * *'),
        (lambda e: e.daily_at('13:45'), '45 13 * * *'),
        (lambda e: e.at('8:05'), '5 8 * * *'),
        (lambda e: e.twice_daily(1, 13), '0 1,13 * * *'),
        (lambda e: e.weekly(), '0 0 * * 0'),
        (lambda e: e.weekly_on(5, '03:00'), '0 3 * * 5'),
        (lambda e: e.monthly(), '0 0 1 * *'),
        (lambda e: e.monthly_on(15, '09:30'), '30 9 15 * *'),
        (lambda e: e.quarterly(), '0 0 1 1-12/3 *'),
        (lambda e: e.yearly(), '0 0 1 1 *'),
        (lambda e: e.yearly_on(6, 1, '12:00'), '0 12 1 6 *'),
        (lambda e: e.daily_at('09:00').weekdays(), '0 9 * * 1-5'),
        (lambda e: e.hourly().weekends(), '0 * * * 0,6'),
        (lambda e: e.daily_at('07:00').mondays(), '0 7 * * 1'),
        (lambda e: e.daily().days(1, 3, 5), '0 0 * * 1,3,5'),
        (lambda e: e.cron('*/7 2-4 * * *'), '*/7 2-4 * * *'),
    ],
)
def test_frequency_helpers(build, expression):
    assert build(Event(tasks.add)).expression == expression


def test_invalid_schedule_input():
    with pytest.raises(QueueError):
        Event(tasks.add).cron('nope')
    with pytest.raises(QueueError):
        Event(tasks.add).daily_at('25:00')
    with pytest.raises(QueueError, match='needs a @task function'):
        schedule.task(print)


# -- running ------------------------------------------------------------------
def test_entry_is_queued_once_per_slot(state_in, on, run):
    schedule.task(on(tasks.add), 1, b=2).daily_at('01:00').name('Send Email')

    assert schedule.run(utc('2026-03-10 00:59:30')) == []
    ran = schedule.run(utc('2026-03-10 01:00:05'))
    assert labels(ran) == ['Send Email']
    assert ran[0][1] == utc('2026-03-10 01:00:00')

    # cron fired twice, or a second server runs the scheduler too
    assert schedule.run(utc('2026-03-10 01:00:40')) == []
    assert schedule.run(utc('2026-03-10 14:00:00')) == []

    assert labels(schedule.run(utc('2026-03-11 01:00:02'))) == ['Send Email']

    assert run() == 2
    assert tasks.calls == [('add', 1, 2), ('add', 1, 2)]


def test_new_entry_does_not_run_for_a_slot_long_gone(state_in, on):
    schedule.task(on(tasks.add), 1, 2).daily_at('01:00')
    # added at noon: today's 01:00 is not made up for
    assert schedule.run(utc('2026-03-10 12:00:00')) == []
    assert labels(schedule.run(utc('2026-03-11 01:00:00'))) == ['tests.tasks.add']


def test_slot_missed_briefly_still_runs_but_not_after_long_downtime(state_in, on):
    schedule.task(on(tasks.add), 1, 2).hourly()
    assert len(schedule.run(utc('2026-03-10 01:00:00'))) == 1
    # the scheduler was restarting at 02:00 and is back 3 minutes later
    assert len(schedule.run(utc('2026-03-10 02:03:00'))) == 1
    # down from before 03:00 until 03:30: that slot is skipped
    assert schedule.run(utc('2026-03-10 03:30:00')) == []
    assert len(schedule.run(utc('2026-03-10 04:00:10'))) == 1


def test_without_overlapping_skips_while_the_last_run_is_unfinished(
    state_in, on, run, backend
):
    schedule.task(on(tasks.add), 1, 2).every_minute().without_overlapping()

    assert len(schedule.run(utc('2026-03-10 01:00:00'))) == 1
    # still queued a minute later: skip
    assert schedule.run(utc('2026-03-10 01:01:00')) == []
    backend.reserve(['*'], 'w')  # ... and while it runs
    assert schedule.run(utc('2026-03-10 01:02:00')) == []
    assert backend.stats()['ready'] == 0


def test_without_overlapping_resumes_after_the_run(state_in, on, run):
    schedule.task(on(tasks.add), 1, 2).every_minute().without_overlapping()
    schedule.run(utc('2026-03-10 01:00:00'))
    run()
    assert len(schedule.run(utc('2026-03-10 01:01:00'))) == 1


def test_overlapping_is_allowed_by_default(state_in, on, backend):
    schedule.task(on(tasks.add), 1, 2).every_minute()
    schedule.run(utc('2026-03-10 01:00:00'))
    schedule.run(utc('2026-03-10 01:01:00'))
    assert backend.stats()['ready'] == 2


def test_scheduled_command(state_in, on, run, alias, monkeypatch):
    from b_queue import tasks as builtin

    monkeypatch.setattr(builtin, 'run_command', on(builtin.run_command))
    schedule.command('check', '--deploy', fail_level='CRITICAL').daily()

    ran = schedule.run(utc('2026-03-10 00:00:00'))
    assert labels(ran) == ['command:check --deploy']
    assert run() == 1
    from django.tasks import task_backends

    result = task_backends[alias].get_result(
        task_backends[alias].get_record(_last_job_id(task_backends[alias])).id
    )
    assert result.status == TaskResultStatus.SUCCESSFUL


def _last_job_id(backend):
    if backend.alias == 'default':
        from b_queue.models import Job

        return str(Job.objects.latest('enqueued_at').id)
    key = backend.client.keys('{b_queue_test}:job:*')[0]
    return key.rsplit(':', 1)[1]


def test_filters(state_in, on):
    allowed = {'value': False}
    schedule.task(on(tasks.add), 1, 2).hourly().between('08:00', '17:00').name('office')
    schedule.task(on(tasks.add), 1, 2).hourly().unless_between('08:00', '17:00').name(
        'night'
    )
    schedule.task(on(tasks.add), 1, 2).hourly().when(lambda: allowed['value']).name(
        'when'
    )
    schedule.task(on(tasks.add), 1, 2).hourly().skip(lambda: allowed['value']).name(
        'skip'
    )

    assert labels(schedule.run(utc('2026-03-10 09:00:00'))) == ['office', 'skip']
    allowed['value'] = True
    assert labels(schedule.run(utc('2026-03-10 22:00:00'))) == ['night', 'when']


def test_entry_timezone(state_in, on):
    schedule.task(on(tasks.add), 1, 2).daily_at('08:00').timezone('Asia/Baghdad')
    # 08:00 in Baghdad is 05:00 UTC
    assert schedule.run(utc('2026-03-09 08:00:00')) == []
    ran = schedule.run(utc('2026-03-10 05:00:00'))
    assert len(ran) == 1
    assert ran[0][1] == datetime(2026, 3, 10, 8, 0, tzinfo=ZoneInfo('Asia/Baghdad'))


def test_times_follow_the_project_timezone(state_in, on, settings):
    settings.TIME_ZONE = 'Asia/Baghdad'
    from django.utils import timezone

    with timezone.override('Asia/Baghdad'):
        schedule.task(on(tasks.add), 1, 2).daily_at('01:00')
        assert len(schedule.run(utc('2026-03-09 22:00:00'))) == 1


def test_on_queue(state_in, on, backend):
    schedule.task(on(tasks.add), 1, 2).every_minute().on_queue('reports')
    schedule.run(utc('2026-03-10 01:00:00'))
    assert backend.stats()['queues'] == {'reports': 1}


def test_duplicate_entries_need_names(db):
    schedule.task(tasks.add, 1, 2).daily()
    schedule.task(tasks.add, 3, 4).hourly()
    with pytest.raises(QueueError, match='unique names'):
        schedule.load()

    schedule.events[1].name('second')
    assert len(schedule.load().events) == 2


def test_state_of_removed_entries_is_dropped(state_in, on, backend):
    schedule.task(on(tasks.add), 1, 2).every_minute().name('old')
    schedule.run(utc('2026-03-10 01:00:00'))
    schedule.clear()
    schedule.task(on(tasks.add), 1, 2).every_minute().name('new')
    schedule.run(utc('2026-03-10 01:01:00'))

    # re-added later, "old" is a new entry again and runs for the current slot
    schedule.task(on(tasks.add), 1, 2).every_minute().name('old')
    assert 'old' in labels(schedule.run(utc('2026-03-10 01:02:00')))


def test_one_broken_entry_does_not_stop_the_others(state_in, on, monkeypatch):
    broken = schedule.task(on(tasks.add), 1, 2).every_minute().name('broken')
    schedule.task(on(tasks.add), 1, 2).every_minute().name('fine')
    monkeypatch.setattr(broken, 'dispatch', lambda: 1 / 0)
    assert labels(schedule.run(utc('2026-03-10 01:00:00'))) == ['fine']


def test_next_due():
    event = Event(tasks.add).daily_at('01:00')
    assert event.next_due(utc('2026-03-10 12:00:00')) == utc('2026-03-11 01:00:00')
    assert event.due_at(utc('2026-03-10 12:00:00')) == utc('2026-03-10 01:00:00')
    assert timedelta(0) == event.due_at(utc('2026-03-10 01:00:59')) - utc(
        '2026-03-10 01:00:00'
    )


def test_works_with_djangos_immediate_backend(db, settings):
    settings.TASKS = {
        'default': {'BACKEND': 'django.tasks.backends.immediate.ImmediateBackend'}
    }
    schedule.task(tasks.add, 1, 2).every_minute().without_overlapping()
    assert len(schedule.run(utc('2026-03-10 01:00:00'))) == 1
    assert schedule.run(utc('2026-03-10 01:00:30')) == []
    assert tasks.calls == [('add', 1, 2)]  # ran on the spot
