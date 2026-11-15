"""Enqueueing and running jobs; every test runs on both backends."""

import pickle
import threading
from datetime import timedelta

import pytest
from django.contrib.auth.models import User
from django.db import transaction
from django.tasks import TaskResultStatus, task_backends
from django.tasks.exceptions import TaskResultDoesNotExist
from django.tasks.signals import task_enqueued, task_finished, task_started
from django.utils import timezone

from b_queue import QueueError, QueueTask
from b_queue.record import RUNNING, SUCCESSFUL
from b_queue.task import task as queue_task
from tests import tasks


def test_enqueue_run_and_read_the_result(on, run):
    result = on(tasks.add).enqueue(2, b=3)
    assert result.status == TaskResultStatus.READY
    assert tasks.calls == []  # nothing ran in the caller

    assert run() == 1

    result.refresh()
    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value == 5
    assert result.attempts == 1
    assert result.worker_ids == ['test-worker']
    assert result.started_at and result.finished_at
    assert tasks.calls == [('add', 2, 3)]
    assert run() == 0  # and it does not run twice


def test_django_task_decorator_builds_queue_tasks(on, run):
    assert isinstance(tasks.plain, QueueTask)
    assert tasks.plain_with_options.tries == 2

    result = on(tasks.plain).enqueue('x')
    run()
    result.refresh()
    assert result.return_value == 'x'


def test_laravel_style_aliases(on, run):
    on(tasks.add).dispatch(1, 1)
    assert tasks.add(1, 2) == 3  # called directly: runs now, no queue
    assert tasks.add.dispatch_sync(1, 3) == 4
    assert run() == 1


def test_get_result_through_the_task(on, backend):
    result = on(tasks.add).enqueue(1, 2)
    assert on(tasks.add).get_result(result.id).id == result.id

    with pytest.raises(TaskResultDoesNotExist):
        backend.get_result('00000000-0000-0000-0000-000000000000')
    with pytest.raises(TaskResultDoesNotExist):
        backend.get_result('not-an-id')


def test_async_task(on, run):
    result = on(tasks.async_add).enqueue(2, 2)
    run()
    result.refresh()
    assert result.return_value == 4


def test_a_job_can_enqueue_more_jobs(on, run):
    on(tasks.chain).enqueue(2)
    assert run() == 3
    assert tasks.calls == [('chain', 2), ('chain', 1), ('chain', 0)]


def test_arguments_must_be_json(on, db):
    with pytest.raises(TypeError, match='pass ids, not model instances'):
        on(tasks.add).enqueue(User(username='x'), 1)


# -- delay, priority, queues ------------------------------------------------
def test_delayed_job_waits(on, run, backend):
    on(tasks.add).delay(minutes=10).enqueue(1, 2)
    assert run() == 0
    assert backend.stats()['delayed'] == 1
    assert run(include_delayed=True) == 1


def test_delay_accepts_timedelta_and_datetime():
    soon = tasks.add.delay(timedelta(hours=1)).run_after
    assert timedelta(minutes=59) < soon - timezone.now() <= timedelta(hours=1)
    moment = timezone.now() + timedelta(days=1)
    assert tasks.add.delay(moment).run_after == moment


def test_higher_priority_runs_first(on, run):
    on(tasks.add).enqueue(1, 0)
    on(tasks.add).with_priority(10).enqueue(2, 0)
    on(tasks.add).with_priority(-5).enqueue(3, 0)
    on(tasks.add).enqueue(4, 0)
    run()
    assert [call[1] for call in tasks.calls] == [2, 1, 4, 3]


def test_queues_are_worked_in_the_order_given(on, run, backend):
    on(tasks.add).on_queue('low').enqueue(1, 0)
    on(tasks.add).on_queue('high').enqueue(2, 0)
    on(tasks.add).enqueue(3, 0)
    assert backend.stats()['queues'] == {'default': 1, 'high': 1, 'low': 1}

    assert run(queues=['high', 'low']) == 2
    assert [call[1] for call in tasks.calls] == [2, 1]
    assert run() == 1  # all queues: the one left on "default"


# -- failures and retries -----------------------------------------------------
def test_failed_job_is_recorded_not_raised(on, run, backend):
    result = on(tasks.boom).enqueue()
    assert run() == 1

    result.refresh()
    assert result.status == TaskResultStatus.FAILED
    assert result.errors[0].exception_class is ValueError
    assert 'boom' in result.errors[0].traceback
    with pytest.raises(ValueError):
        result.return_value
    assert [record.id for record in backend.failed_jobs()] == [result.id]


def test_retries_with_backoff_until_it_works(on, run, backend):
    tasks.failures_left['flaky'] = 2
    result = on(tasks.flaky).enqueue()

    assert run() == 1  # first attempt fails, the retry waits out its backoff
    record = backend.get_record(result.id)
    assert record.status == 'READY'
    wait = (record.available_at - timezone.now()).total_seconds()
    assert 8 < wait <= 10
    assert run() == 0

    assert run(include_delayed=True) == 2  # second fails, third works
    result.refresh()
    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.return_value == 'ok'
    assert result.attempts == 3
    assert len(result.errors) == 2
    assert backend.failed_jobs() == []


def test_fails_for_good_when_tries_run_out(on, run):
    tasks.failures_left['flaky'] = 99
    result = on(tasks.flaky).enqueue()
    assert run(include_delayed=True) == 3
    result.refresh()
    assert result.status == TaskResultStatus.FAILED
    assert result.attempts == 3
    assert len(result.errors) == 3


def test_fail_exception_skips_the_tries_left(on, run):
    result = on(tasks.give_up).enqueue()
    assert run(include_delayed=True) == 1
    result.refresh()
    assert result.status == TaskResultStatus.FAILED


def test_retry_exception_sets_its_own_delay(on, run, backend):
    result = on(tasks.retry_later).enqueue()
    run()
    record = backend.get_record(result.id)
    assert record.status == 'READY'
    assert 3 < (record.available_at - timezone.now()).total_seconds() <= 5

    run(include_delayed=True)  # it counts as an attempt: no tries left now
    result.refresh()
    assert result.status == TaskResultStatus.FAILED


def test_context_knows_the_attempt(on, run):
    result = on(tasks.with_context).enqueue()
    assert run() == 2
    result.refresh()
    assert result.return_value == 2
    assert tasks.calls == [('with_context', 1), ('with_context', 2)]


def test_timeout_interrupts_the_job(on, run):
    result = on(tasks.slow).enqueue(5)
    began = timezone.now()
    run()
    assert timezone.now() - began < timedelta(seconds=3)

    result.refresh()
    assert result.status == TaskResultStatus.FAILED
    assert result.errors[0].exception_class_path == 'b_queue.exceptions.JobTimedOut'
    assert tasks.calls == []


def test_missing_task_code_fails_the_job(on, run, backend):
    result = on(tasks.add).enqueue(1, 2)
    record = backend.reserve(['*'], 'w')
    record.task_path = 'tests.tasks.removed_long_ago'
    from b_queue.worker import Worker

    Worker(backend).process(record)
    assert backend.failed_jobs()[0].id == result.id


def test_failed_jobs_can_be_retried_forgotten_and_flushed(on, run, backend):
    first = on(tasks.boom).enqueue()
    second = on(tasks.boom).enqueue()
    third = on(tasks.boom).enqueue()
    run()
    assert backend.stats()['failed'] == 3

    assert backend.retry_job(first.id) is True
    assert backend.retry_job(first.id) is False  # no longer failed
    assert backend.stats()['failed'] == 2
    assert run() == 1  # ran (and failed) again, with fresh tries
    assert backend.get_record(first.id).attempts == 1

    assert backend.forget_job(second.id) is True
    with pytest.raises(TaskResultDoesNotExist):
        backend.get_result(second.id)

    assert backend.flush_failed() == 2
    assert backend.failed_jobs() == []
    assert third.id  # flushed with the rest


# -- unique jobs --------------------------------------------------------------
def test_unique_job_is_dropped_while_one_is_unfinished(on, run):
    first = on(tasks.unique_job).enqueue(1)
    again = on(tasks.unique_job).enqueue(1)
    other = on(tasks.unique_job).enqueue(2)  # other arguments: its own job

    assert again.id == first.id  # the job already queued is handed back
    assert other.id != first.id
    assert run() == 2

    # finished: the next enqueue goes through
    assert on(tasks.unique_job).enqueue(1).id != first.id
    assert run() == 1


def test_unique_job_keeps_blocking_while_it_runs_and_retries(on, backend):
    first = on(tasks.unique_job).enqueue(1)
    record = backend.reserve(['*'], 'w')
    assert on(tasks.unique_job).enqueue(1).id == first.id  # running

    record.status = 'READY'  # as if it failed and waits for a retry
    backend.save(record)
    assert on(tasks.unique_job).enqueue(1).id == first.id


def test_unique_until_start_lets_one_more_queue_up_mid_run(on, backend):
    first = on(tasks.recalculate).enqueue(7)
    # the key function only looks at the customer id
    assert on(tasks.recalculate).enqueue(7, note='x').id == first.id
    assert on(tasks.recalculate).enqueue(8).id != first.id

    backend.reserve(['*'], 'w')  # customer 7 starts running
    second = on(tasks.recalculate).enqueue(7)
    assert second.id != first.id  # a change made mid-run is not missed
    assert on(tasks.recalculate).enqueue(7).id == second.id  # but only one


def test_unique_for_expires(on, backend, monkeypatch):
    first = on(tasks.unique_for_a_minute).enqueue()
    assert on(tasks.unique_for_a_minute).enqueue().id == first.id

    if backend.alias == 'redis':
        # Redis expires the key itself
        ttl = backend.client.ttl(
            backend._key('unique', backend.get_record(first.id).unique_key)
        )
        assert 0 < ttl <= 60
        backend.client.delete(
            backend._key('unique', backend.get_record(first.id).unique_key)
        )
    else:
        from b_queue.models import Job

        Job.objects.filter(id=first.id).update(
            enqueued_at=timezone.now() - timedelta(seconds=61)
        )
    assert on(tasks.unique_for_a_minute).enqueue().id != first.id


def test_unique_enqueue_does_not_break_the_callers_transaction(on, db):
    with transaction.atomic():
        on(tasks.unique_job).enqueue(1)
        on(tasks.unique_job).enqueue(1)  # duplicate key inside the transaction
        User.objects.create(username='still-works')
    assert User.objects.filter(username='still-works').exists()


def test_concurrent_unique_enqueues_queue_one_job(on, backend, alias):
    if alias == 'default':
        pytest.skip('the database variant is in test_concurrency.py')
    task = on(tasks.unique_job)
    ids = set()

    def enqueue():
        ids.add(backend.enqueue(task, (1,), {}).id)

    threads = [threading.Thread(target=enqueue) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(ids) == 1
    assert backend.stats()['ready'] == 1


# -- without overlapping ------------------------------------------------------
def test_without_overlapping_holds_a_job_back(on, run, backend):
    first = on(tasks.exclusive).enqueue()
    second = on(tasks.exclusive).enqueue()

    # a worker elsewhere is running the first one
    running = backend.reserve(['*'], 'other-worker')
    assert running.id == first.id
    assert backend.acquire_lock(running.overlap_key, running.id, 60)

    assert run() == 1  # the second was looked at, but not run
    assert tasks.calls == []
    held = backend.get_record(second.id)
    assert held.status == 'READY'
    assert held.attempts == 0  # being held back is not an attempt
    assert held.available_at > timezone.now()

    backend.release_lock(running.overlap_key, running.id)
    assert run(include_delayed=True) == 1
    assert tasks.calls == [('exclusive',)]
    assert backend.get_record(second.id).status == SUCCESSFUL


def test_lock_is_exclusive_and_expires(backend):
    assert backend.acquire_lock('k', 'a', 60) is True
    assert backend.acquire_lock('k', 'b', 60) is False
    backend.release_lock('k', 'b')  # not the owner: nothing happens
    assert backend.acquire_lock('k', 'b', 60) is False
    backend.release_lock('k', 'a')
    assert backend.acquire_lock('k', 'b', 60) is True


def test_lock_is_released_after_the_job(on, run, backend):
    on(tasks.exclusive).enqueue()
    on(tasks.exclusive).enqueue()
    assert run() == 2
    assert tasks.calls == [('exclusive',), ('exclusive',)]


# -- crashed workers ----------------------------------------------------------
def test_lost_job_without_tries_left_is_failed(on, backend):
    result = on(tasks.add).enqueue(1, 2)
    record = backend.reserve(['*'], 'dead-worker')
    assert record.status == RUNNING

    assert backend.recover_lost() == 0  # still within its timeout
    later = timezone.now() + timedelta(seconds=record.timeout + backend.lost_grace + 1)
    assert backend.recover_lost(now=later) == 1

    result.refresh()
    assert result.status == TaskResultStatus.FAILED
    assert result.errors[0].exception_class_path == 'b_queue.exceptions.JobTimedOut'
    assert backend.stats()['running'] == 0

    # the worker was only slow, and finishes now: its result must not win
    record.status = SUCCESSFUL
    assert backend.save(record) is False
    assert backend.get_record(result.id).status == 'FAILED'


def test_lost_job_with_tries_left_runs_again(on, run, backend):
    result = on(tasks.flaky).enqueue()
    record = backend.reserve(['*'], 'dead-worker')
    later = timezone.now() + timedelta(seconds=record.timeout + backend.lost_grace + 1)
    assert backend.recover_lost(now=later) == 1

    assert run(include_delayed=True) == 1
    result.refresh()
    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.attempts == 2


def test_a_job_is_reserved_by_one_worker_only(on, backend, alias):
    if alias == 'default':
        pytest.skip('the database variant is in test_concurrency.py')
    for number in range(30):
        on(tasks.add).enqueue(number, 0)

    taken = []

    def work(name):
        while record := backend.reserve(['*'], name):
            taken.append(record.id)

    threads = [threading.Thread(target=work, args=(f'w{n}',)) for n in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert len(taken) == 30
    assert len(set(taken)) == 30


# -- transactions -------------------------------------------------------------
def test_database_job_is_rolled_back_with_the_transaction(db):
    backend = task_backends['default']
    try:
        with transaction.atomic():
            tasks.add.enqueue(1, 2)
            assert backend.stats()['ready'] == 1
            raise RuntimeError
    except RuntimeError:
        pass
    assert backend.stats()['ready'] == 0


def test_redis_job_waits_for_the_commit(db, django_capture_on_commit_callbacks):
    from tests.conftest import _prepare_redis

    _prepare_redis()
    backend = task_backends['redis_on_commit']
    task = tasks.add.using(backend='redis_on_commit')

    with django_capture_on_commit_callbacks(execute=True):
        result = task.enqueue(1, 2)
        assert backend.stats()['ready'] == 0  # not visible to workers yet
    assert backend.stats()['ready'] == 1
    assert backend.get_result(result.id).id == result.id

    try:
        with transaction.atomic():
            task.enqueue(3, 4)
            raise RuntimeError
    except RuntimeError:
        pass
    assert backend.stats()['ready'] == 1  # the rolled back one was never sent


# -- housekeeping -------------------------------------------------------------
def test_prune_and_clear(on, run, backend, alias):
    on(tasks.add).enqueue(1, 2)
    on(tasks.boom).enqueue()
    run()
    on(tasks.add).enqueue(3, 4)
    on(tasks.add).delay(hours=1).enqueue(5, 6)

    future = timezone.now() + timedelta(days=1)
    if alias == 'default':
        assert backend.prune(future) == 1  # the successful one
    else:
        assert backend.prune(future) == 0  # in Redis those expire by themselves
    assert backend.stats()['failed'] == 1
    assert backend.prune(future, include_failed=True) == 1
    assert backend.stats()['failed'] == 0

    assert backend.clear() == 2
    assert backend.stats() == {
        'ready': 0,
        'delayed': 0,
        'running': 0,
        'failed': 0,
        'queues': {},
    }


def test_successful_redis_jobs_expire(db):
    from tests.conftest import _prepare_redis

    _prepare_redis()
    backend = task_backends['redis']
    result = tasks.add.using(backend='redis').enqueue(1, 2)
    from b_queue.testing import run_jobs

    run_jobs(backend='redis')
    ttl = backend.client.ttl(backend._key('job', result.id))
    assert 0 < ttl <= backend.keep_results


def test_signals(on, run):
    seen = []

    def record(name):
        def receiver(sender, task_result, **kwargs):
            seen.append((name, task_result.status.value))

        return receiver

    receivers = [record('enqueued'), record('started'), record('finished')]
    task_enqueued.connect(receivers[0])
    task_started.connect(receivers[1])
    task_finished.connect(receivers[2])
    try:
        on(tasks.add).enqueue(1, 2)
        tasks.failures_left['flaky'] = 1
        on(tasks.flaky).enqueue()
        run(include_delayed=True)
    finally:
        task_enqueued.disconnect(receivers[0])
        task_started.disconnect(receivers[1])
        task_finished.disconnect(receivers[2])

    assert seen == [
        ('enqueued', 'READY'),
        ('enqueued', 'READY'),
        ('started', 'RUNNING'),
        ('finished', 'SUCCESSFUL'),
        ('started', 'RUNNING'),  # flaky fails: no "finished" for a retry
        ('started', 'RUNNING'),
        ('finished', 'SUCCESSFUL'),
    ]


# -- the task class -----------------------------------------------------------
def test_task_with_a_lambda_can_be_pickled():
    copy = pickle.loads(pickle.dumps(tasks.recalculate.on_queue('other')))
    assert copy.queue_name == 'other'
    assert copy.func is tasks.recalculate.func


def test_invalid_options_are_rejected_when_the_task_is_declared():
    def nothing():
        pass

    with pytest.raises(QueueError, match='tries'):
        queue_task(tries=0)(tasks.add.func)
    with pytest.raises(QueueError, match='unique_until'):
        queue_task(unique_until='never')(tasks.add.func)
    with pytest.raises(Exception, match='module level'):
        queue_task(nothing)
