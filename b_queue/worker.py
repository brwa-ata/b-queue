"""The worker: takes jobs off the queue and runs them, one at a time.

`manage.py queue_work` wraps this. Run several workers (or --processes N)
for parallelism; they never take the same job.
"""

from __future__ import annotations

import json
import logging
import signal
import socket
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from traceback import format_exception

from django.db import connections
from django.tasks import TaskContext
from django.tasks.signals import task_finished, task_started
from django.utils import timezone

from b_queue.backends.base import QueueBackend
from b_queue.context import activate
from b_queue.exceptions import Fail, JobTimedOut, Retry
from b_queue.record import FAILED, READY, SUCCESSFUL, JobRecord, build_result, load_task

logger = logging.getLogger('b_queue')

MAINTENANCE_EVERY = 60  # seconds between lost-job checks
PRUNE_EVERY = 60 * 60


@contextmanager
def time_limit(seconds: float):
    """Raise JobTimedOut inside the block once `seconds` have passed.

    Needs SIGALRM and the main thread (i.e. a normal worker on Linux/macOS).
    Elsewhere the block is not interrupted; a job that hangs is then caught
    by lost-job recovery instead.
    """
    usable = (
        hasattr(signal, 'SIGALRM')
        and threading.current_thread() is threading.main_thread()
    )
    if not usable:
        yield
        return

    def on_alarm(signum, frame):
        raise JobTimedOut(f'Timed out after {seconds}s')

    previous = signal.signal(signal.SIGALRM, on_alarm)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def release_connections() -> None:
    """A long-lived process must not keep dead or expired database
    connections (CONN_MAX_AGE). One inside a transaction is left alone: that
    is a test running jobs in its own transaction."""
    for connection in connections.all(initialized_only=True):
        if not connection.in_atomic_block:
            connection.close_if_unusable_or_obsolete()


def class_path(error: BaseException) -> str:
    cls = type(error)
    return f'{cls.__module__}.{cls.__qualname__}'


class Worker:
    def __init__(
        self,
        backend: QueueBackend,
        *,
        queues: list[str] | None = None,
        sleep: float = 1.0,
        stop_when_empty: bool = False,
        max_jobs: int | None = None,
        max_time: float | None = None,
        worker_id: str | None = None,
        include_delayed: bool = False,
        output=None,
    ):
        self.backend = backend
        self.queues = queues or ['*']
        self.sleep = sleep
        self.stop_when_empty = stop_when_empty
        self.max_jobs = max_jobs
        self.max_time = max_time
        self.worker_id = worker_id or f'{socket.gethostname()}-{id(self):x}'[:64]
        self.include_delayed = include_delayed  # tests: ignore run_after/backoff
        self.output = output or (lambda line: None)
        self.processed = 0
        self._stop = threading.Event()

    # -- loop -------------------------------------------------------------
    def stop(self, *args) -> None:
        """Finish the current job, then exit (SIGTERM / first Ctrl+C)."""
        self._stop.set()

    def run(self) -> int:
        started = time.monotonic()
        next_maintenance = next_prune = 0.0

        while not self._stop.is_set():
            release_connections()

            clock = time.monotonic()
            if clock >= next_maintenance:
                next_maintenance = clock + MAINTENANCE_EVERY
                self.maintain(prune=clock >= next_prune)
                if clock >= next_prune:
                    next_prune = clock + PRUNE_EVERY

            record = self.reserve()
            if record is None:
                if self.stop_when_empty:
                    break
                self._stop.wait(self.sleep)
            else:
                try:
                    self.process(record)
                except Exception:
                    # e.g. the queue went away while saving the outcome: the
                    # job is picked up again by lost-job recovery
                    logger.exception('Could not finish job %s', record.id)
                self.processed += 1

            if self.max_jobs and self.processed >= self.max_jobs:
                break
            if self.max_time and time.monotonic() - started >= self.max_time:
                break

        return self.processed

    def reserve(self) -> JobRecord | None:
        now = None
        if self.include_delayed:
            now = timezone.now() + timedelta(days=36500)
        try:
            return self.backend.reserve(self.queues, self.worker_id, now)
        except Exception:
            # the database or Redis is away: keep the worker alive
            logger.exception('Could not reach the queue')
            self._stop.wait(max(self.sleep, 1))
            return None

    def maintain(self, prune: bool = False) -> None:
        try:
            recovered = self.backend.recover_lost()
            if recovered:
                logger.warning('Recovered %s lost job(s)', recovered)
            if prune and self.backend.keep_results is not None:
                self.backend.prune(
                    timezone.now() - timedelta(seconds=self.backend.keep_results)
                )
        except Exception:
            logger.exception('Queue maintenance failed')

    # -- one job ----------------------------------------------------------
    def process(self, record: JobRecord) -> None:
        backend = self.backend

        try:
            task = load_task(record.task_path)
        except Exception as error:
            # the code is gone or renamed: retrying cannot help
            self._failed(record, None, error, final=True)
            return

        lock = record.overlap_key
        if lock and not backend.acquire_lock(
            lock, record.id, record.timeout + backend.lost_grace
        ):
            self._hold_back(record)
            return

        began = time.monotonic()
        self._say(record, 'RUNNING')
        try:
            result = build_result(record, task)
            task_started.send(sender=type(backend), task_result=result)

            args = record.args
            if task.takes_context:
                args = [TaskContext(task_result=result), *args]
            with activate(record.context), time_limit(record.timeout):
                value = task.call(*args, **record.kwargs)

            # a Decimal or date in the return value must not turn a job that
            # did its work into a failed one
            record.return_value = json.loads(json.dumps(value, default=str))
        except BaseException as error:
            self._failed(record, task, error, began=began)
            if not isinstance(error, Exception):
                raise  # KeyboardInterrupt, SystemExit: stop the worker
        else:
            record.status = SUCCESSFUL
            record.finished_at = timezone.now()
            record.timeout_at = None
            if self._save(record):
                self._say(record, 'DONE', began)
                self._send_finished(record, task)
        finally:
            if lock:
                backend.release_lock(lock, record.id)

    def _failed(self, record, task, error, *, final=False, began=None) -> None:
        now = timezone.now()
        record.add_error(class_path(error), ''.join(format_exception(error)))
        record.timeout_at = None

        retry = (
            not final and not isinstance(error, Fail) and record.attempts < record.tries
        )
        if retry:
            wait = getattr(error, 'delay', None) if isinstance(error, Retry) else None
            if wait is None:
                wait = record.backoff_for(record.attempts)
            record.status = READY
            record.available_at = now + timedelta(seconds=wait)
            if self._save(record):
                self._say(record, f'RETRY in {wait:g}s', began)
                logger.warning(
                    'Job %s %s failed (attempt %s of %s), retrying in %ss: %r',
                    record.task_path,
                    record.id,
                    record.attempts,
                    record.tries,
                    wait,
                    error,
                )
            return

        record.status = FAILED
        record.finished_at = now
        if self._save(record):
            self._say(record, 'FAIL', began)
            if task is None:
                logger.error('Job %s %s failed: %r', record.task_path, record.id, error)
            else:
                # sent inside the except block: Django's own receiver logs
                # the traceback from there
                self._send_finished(record, task)

    def _hold_back(self, record) -> None:
        """Another job with the same without_overlapping key is running:
        look at this one again shortly. It was not run, so the attempt is
        not counted."""
        record.status = READY
        record.worker_ids.pop()
        record.timeout_at = None
        record.available_at = timezone.now() + timedelta(
            seconds=self.backend.overlap_retry
        )
        self._save(record)

    def _save(self, record) -> bool:
        if self.backend.save(record):
            return True
        logger.warning(
            'Job %s %s ran past its timeout and was taken over; its result is dropped',
            record.task_path,
            record.id,
        )
        return False

    def _send_finished(self, record, task) -> None:
        try:
            task_finished.send(
                sender=type(self.backend), task_result=build_result(record, task)
            )
        except Exception:
            logger.exception('A task_finished receiver failed')

    def _say(self, record, state, began=None) -> None:
        took = f' {(time.monotonic() - began) * 1000:.0f}ms' if began else ''
        self.output(
            f'{datetime.now():%Y-%m-%d %H:%M:%S} {record.task_path} '
            f'{record.id[:8]} {state}{took}'
        )
