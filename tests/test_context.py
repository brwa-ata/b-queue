"""The optional context hook (multi-tenancy), on both backends."""

import pickle
from datetime import UTC, datetime

import pytest
from django.db import transaction
from django.tasks import TaskResultStatus, task_backends

from b_queue import schedule
from b_queue.backends.base import digest
from tests import tasks
from tests.tenancy import current, tenant

NOW = datetime(2026, 3, 10, 1, 0, tzinfo=UTC)


@pytest.fixture
def tenancy(alias, settings):
    settings.B_QUEUE = {
        'SCHEDULE_MODULES': [],
        'SCHEDULE_BACKEND': alias,
        'CONTEXT': 'tests.tenancy.QueueContext',
    }


# -- off: nothing changes -----------------------------------------------------
def test_off_by_default_nothing_is_captured_or_activated(on, run, backend):
    with tenant(5):  # the project has tenants, but no provider is configured
        result = on(tasks.whoami).enqueue()
        unique = on(tasks.unique_job).enqueue(1)

    assert backend.get_record(result.id).context is None
    # keys are exactly what they were before the feature existed
    assert backend.get_record(unique.id).unique_key == digest(
        'unique', 'tests.tasks.unique_job', [[1], {}]
    )
    run()
    assert ('whoami', None) in tasks.calls


def test_off_a_stored_context_is_ignored_by_the_worker(on, run):
    on(tasks.whoami).with_context(tenant=9).enqueue()
    run()
    assert tasks.calls == [('whoami', None)]


# -- on -----------------------------------------------------------------------
def test_job_runs_inside_the_tenant_it_was_queued_from(tenancy, on, run, backend):
    with tenant(5):
        five = on(tasks.whoami).enqueue()
    with tenant(6):
        six = on(tasks.whoami).enqueue()
    nobody = on(tasks.whoami).enqueue()

    assert backend.get_record(five.id).context == {'tenant': 5}
    assert backend.get_record(nobody.id).context is None

    assert run() == 3
    assert tasks.calls == [('whoami', 5), ('whoami', 6), ('whoami', None)]
    assert current.get() is None  # the worker left the tenant again

    six.refresh()
    assert six.return_value == 6
    assert backend.get_result(six.id).task.context == {'tenant': 6}


def test_explicit_context_wins_over_the_current_one(tenancy, on, run):
    with tenant(5):
        on(tasks.whoami).with_context(tenant=7).enqueue()
        on(tasks.whoami).without_context().enqueue()
        on(tasks.global_job).enqueue()  # declared with context=False
    run()
    assert tasks.calls == [('whoami', 7), ('whoami', None), ('global_job', None)]


def test_unique_is_per_tenant(tenancy, on, run):
    with tenant(5):
        first = on(tasks.unique_job).enqueue(1)
        assert on(tasks.unique_job).enqueue(1).id == first.id  # same tenant: dropped
    with tenant(6):
        other = on(tasks.unique_job).enqueue(1)
    assert other.id != first.id  # another tenant: its own job
    assert run() == 2


def test_without_overlapping_is_per_tenant(tenancy, on, backend):
    with tenant(5):
        a = on(tasks.exclusive).enqueue()
        b = on(tasks.exclusive).enqueue()
    with tenant(6):
        c = on(tasks.exclusive).enqueue()

    keys = [backend.get_record(result.id).overlap_key for result in (a, b, c)]
    assert keys[0] == keys[1] != keys[2]


def test_retry_keeps_the_tenant(tenancy, on, run):
    tasks.failures_left['flaky'] = 1
    with tenant(5):
        result = on(tasks.flaky).enqueue()
    run(include_delayed=True)
    result.refresh()
    assert result.status == TaskResultStatus.SUCCESSFUL
    assert result.task.context == {'tenant': 5}


def test_tenant_that_cannot_be_activated_fails_the_job(tenancy, on, run, backend):
    result = on(tasks.whoami).with_context(tenant='deleted').enqueue()
    on(tasks.whoami).with_context(tenant=5).enqueue()
    assert run() == 2

    result.refresh()
    assert result.status == TaskResultStatus.FAILED
    assert result.errors[0].exception_class is LookupError
    assert tasks.calls == [('whoami', 5)]  # the worker carried on
    assert backend.failed_jobs()[0].context == {'tenant': 'deleted'}


def test_context_must_be_json(tenancy, on):
    with pytest.raises(TypeError, match='Job context'):
        on(tasks.whoami).with_context(tenant=object()).enqueue()


def test_pickled_task_keeps_its_context():
    copy = pickle.loads(pickle.dumps(tasks.whoami.with_context(tenant=5)))
    assert copy.context == {'tenant': 5}


# -- scheduler ----------------------------------------------------------------
def test_schedule_for_each_queues_one_job_per_tenant(tenancy, on, run):
    tenants = [1, 2]
    schedule.task(on(tasks.whoami)).every_minute().for_each(
        lambda: [{'tenant': tenant_id} for tenant_id in tenants]
    )

    assert len(schedule.run(NOW)) == 1
    assert run() == 2
    assert tasks.calls == [('whoami', 1), ('whoami', 2)]

    tenants.append(3)  # the function is asked again every time
    schedule.run(NOW.replace(minute=1))
    assert run() == 3


def test_schedule_without_overlapping_is_checked_per_tenant(tenancy, on, run, backend):
    schedule.task(on(tasks.whoami)).every_minute().without_overlapping().for_each(
        [{'tenant': 1}, {'tenant': 2}]
    )
    schedule.run(NOW)

    # tenant 1 finishes its run, tenant 2 is still busy a minute later
    record = backend.reserve(['*'], 'w')
    assert record.context == {'tenant': 1}
    record.status = 'SUCCESSFUL'
    backend.save(record)

    schedule.run(NOW.replace(minute=1))
    waiting = backend.stats()['ready']
    assert waiting == 2  # tenant 2's first run + tenant 1's new one
    run()
    assert sorted(call[1] for call in tasks.calls) == [1, 2]


def test_schedule_without_for_each_carries_no_tenant(tenancy, on, run):
    schedule.task(on(tasks.whoami)).every_minute()
    schedule.run(NOW)
    run()
    assert tasks.calls == [('whoami', None)]


def test_schedule_for_each_with_djangos_immediate_backend(db, settings):
    settings.TASKS = {
        'default': {'BACKEND': 'django.tasks.backends.immediate.ImmediateBackend'}
    }
    settings.B_QUEUE = {'SCHEDULE_MODULES': [], 'CONTEXT': 'tests.tenancy.QueueContext'}
    schedule.task(tasks.whoami).every_minute().for_each([{'tenant': 1}, {'tenant': 2}])
    schedule.run(NOW)
    assert tasks.calls == [('whoami', 1), ('whoami', 2)]


# -- separate database per tenant: wait for the tenant's transaction ----------
def test_enqueue_on_commit_can_follow_the_current_tenants_database(
    db, monkeypatch, django_capture_on_commit_callbacks
):
    backend = task_backends['default']
    asked = []

    def tenant_database():
        asked.append(current.get())
        return 'default' if current.get() else None  # the tenant's alias

    monkeypatch.setattr(backend, 'enqueue_on_commit', tenant_database)

    with django_capture_on_commit_callbacks(execute=True):
        with tenant(5):
            tasks.add.enqueue(1, 2)
        assert backend.stats()['ready'] == 0  # waits for the tenant's commit
    assert backend.stats()['ready'] == 1
    assert asked == [5]

    try:
        with transaction.atomic(), tenant(5):
            tasks.add.enqueue(3, 4)
            raise RuntimeError
    except RuntimeError:
        pass
    assert backend.stats()['ready'] == 1  # rolled back: never queued

    tasks.add.enqueue(5, 6)  # outside a tenant: queued right away
    assert backend.stats()['ready'] == 2


def test_enqueue_on_commit_accepts_a_dotted_path(db, monkeypatch):
    backend = task_backends['default']
    monkeypatch.setattr(backend, 'enqueue_on_commit', 'tests.tenancy.no_database')
    tasks.add.enqueue(1, 2)
    assert backend.stats()['ready'] == 1
