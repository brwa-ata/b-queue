from io import StringIO

import pytest
from django.core.management import CommandError, call_command
from django.tasks import task_backends

from b_queue import schedule
from tests import tasks


def command(name, *args, **options):
    out = StringIO()
    call_command(name, *args, stdout=out, stderr=out, **options)
    return out.getvalue()


def test_queue_work_runs_what_is_queued(db):
    tasks.add.enqueue(1, 2)
    tasks.boom.enqueue()
    out = command('queue_work', stop_when_empty=True)

    assert tasks.calls == [('add', 1, 2), ('boom',)]
    assert 'tests.tasks.add' in out and 'DONE' in out and 'FAIL' in out
    assert 'Worker stopped after 2 job(s)' in out


def test_queue_work_options(db):
    tasks.add.on_queue('high').enqueue(1, 0)
    tasks.add.enqueue(2, 0)
    tasks.add.enqueue(3, 0)

    command('queue_work', stop_when_empty=True, queue='high', quiet=True)
    assert tasks.calls == [('add', 1, 0)]
    command('queue_work', max_jobs=1, quiet=True)
    assert len(tasks.calls) == 2


def test_failed_jobs_commands(db):
    first = tasks.boom.enqueue()
    tasks.boom.enqueue()
    command('queue_work', stop_when_empty=True, quiet=True)

    out = command('queue_failed', trace=True)
    assert first.id in out and 'builtins.ValueError' in out
    assert 'raise ValueError' in out

    assert '1 job(s) queued again' in command('queue_retry', first.id)
    with pytest.raises(CommandError, match='Not a failed job'):
        command('queue_retry', first.id)

    command('queue_work', stop_when_empty=True, quiet=True)
    assert '2 job(s) queued again' in command('queue_retry', 'all')
    command('queue_work', stop_when_empty=True, quiet=True)

    assert '1 failed job(s) deleted' in command('queue_forget', first.id)
    assert '1 failed job(s) deleted' in command('queue_flush')
    assert 'No failed jobs.' in command('queue_failed')


def test_status_clear_and_prune(db):
    tasks.add.enqueue(1, 2)
    command('queue_work', stop_when_empty=True, quiet=True)
    tasks.add.enqueue(1, 2)
    tasks.add.delay(hours=1).on_queue('later').enqueue(1, 2)

    out = command('queue_status')
    assert (
        'ready    1' in out and 'delayed  1' in out and 'queue default: 1 ready' in out
    )

    assert '0 job(s) pruned' in command('queue_prune')
    assert '1 job(s) pruned' in command('queue_prune', hours=0)
    assert '1 waiting job(s) deleted' in command('queue_clear', queue='later')
    assert '1 waiting job(s) deleted' in command('queue_clear')


def test_schedule_commands(db):
    assert 'No schedule entries' in command('schedule_list')

    schedule.task(tasks.add, 1, 2).every_minute().name('Every minute')
    schedule.task(tasks.add, 1, 2).daily_at('01:00').name('Send Email')
    out = command('schedule_list')
    assert '* * * * *' in out and 'Send Email' in out and 'next:' in out

    out = command('schedule_run')
    assert 'queued  Every minute' in out and 'Send Email' not in out
    assert task_backends['default'].stats()['ready'] == 1


def test_schedule_run_recovers_lost_jobs(db):
    from datetime import timedelta

    from django.utils import timezone

    from b_queue.models import Job

    tasks.add.enqueue(1, 2)
    task_backends['default'].reserve(['*'], 'dead-worker')
    Job.objects.update(timeout_at=timezone.now() - timedelta(seconds=1))
    assert 'recovered 1 lost job(s)' in command('schedule_run')


def test_wrong_backend_is_reported(db, settings):
    settings.TASKS = {
        'default': {'BACKEND': 'django.tasks.backends.dummy.DummyBackend'}
    }
    with pytest.raises(CommandError, match='not a b_queue backend'):
        command('queue_status')


def test_schedule_modules_are_discovered(db, settings):
    settings.B_QUEUE = {'SCHEDULE_MODULES': None}
    schedule._loaded = False
    try:
        schedule.load()
        # tests/schedules.py (an installed app's module) was imported
        assert [event.label for event in schedule.events] == ['Nightly add']
    finally:
        schedule._loaded = True
