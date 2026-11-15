import os

import pytest
from django.tasks import task_backends

from b_queue import schedule
from tests import tasks

REDIS_ALIASES = ('redis', 'redis_on_commit')


def _prepare_redis():
    """A real Redis when BQ_TEST_REDIS is set, otherwise an in-memory fake."""
    if os.environ.get('BQ_TEST_REDIS'):
        client = task_backends['redis'].client
        keys = client.keys('{b_queue_test}:*')
        if keys:
            client.delete(*keys)
        return

    import fakeredis

    client = fakeredis.FakeRedis(decode_responses=True)
    for alias in REDIS_ALIASES:
        backend = task_backends[alias]
        backend.__dict__.pop('_scripts', None)
        backend.client = client


@pytest.fixture(autouse=True)
def clean_state():
    tasks.calls.clear()
    tasks.failures_left['flaky'] = 0
    schedule.clear()
    yield
    schedule.clear()


@pytest.fixture(params=['default', 'redis'])
def alias(request, db):
    """Every test using this runs once per backend."""
    if request.param == 'redis':
        _prepare_redis()
    tasks.chain_backend['alias'] = request.param
    return request.param


@pytest.fixture
def backend(alias):
    return task_backends[alias]


@pytest.fixture
def on(alias):
    """on(task): the task, sent to the backend under test."""
    return lambda task: task.using(backend=alias)


@pytest.fixture
def run(alias):
    from b_queue.testing import run_jobs

    return lambda **options: run_jobs(backend=alias, **options)
