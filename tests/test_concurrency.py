"""Several workers on one database. Threads need committed rows and row
locks, so this runs on PostgreSQL only:

    BQ_TEST_POSTGRES=postgres://user:pass@localhost/postgres pytest
"""

import threading

import pytest
from django.db import connection, connections
from django.tasks import task_backends

from tests import tasks

pytestmark = [
    pytest.mark.django_db(transaction=True),
    pytest.mark.skipif(
        connection.vendor == 'sqlite', reason='needs PostgreSQL (BQ_TEST_POSTGRES)'
    ),
]


def in_threads(count, target):
    errors = []

    def wrapped(number):
        try:
            target(number)
        except Exception as error:
            errors.append(error)
        finally:
            connections.close_all()

    threads = [threading.Thread(target=wrapped, args=(n,)) for n in range(count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert errors == []


def test_a_job_is_reserved_by_one_worker_only():
    for number in range(40):
        tasks.add.enqueue(number, 0)

    taken = []

    def work(number):
        backend = task_backends['default']
        while record := backend.reserve(['*'], f'w{number}'):
            taken.append(record.id)

    in_threads(8, work)
    assert len(taken) == 40
    assert len(set(taken)) == 40


def test_concurrent_unique_enqueues_queue_one_job():
    ids = []
    in_threads(8, lambda number: ids.append(tasks.unique_job.enqueue(1).id))
    assert len(set(ids)) == 1
    assert task_backends['default'].stats()['ready'] == 1


def test_a_schedule_slot_is_claimed_once():
    from django.utils import timezone

    now = timezone.now().replace(second=0, microsecond=0)
    wins = []

    def claim(number):
        if task_backends['default'].schedule_claim('entry', now, now):
            wins.append(number)

    in_threads(8, claim)
    assert len(wins) == 1
