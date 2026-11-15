"""What the database and Redis backends share.

A backend is a django.tasks backend (enqueue / get_result) plus the storage
operations the worker, the scheduler and the management commands need.
Subclasses only implement storage; the rules (unique keys, defaults,
enqueue-on-commit) live here.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime

from django.db import connections, transaction
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.signals import task_enqueued
from django.utils import timezone
from django.utils.module_loading import import_string

from b_queue.context import capture
from b_queue.record import JobRecord, build_result
from b_queue.task import QueueTask, validate_options

DAY = 24 * 60 * 60


def to_json(value, what: str):
    """Round-trip through JSON so a bad argument fails at enqueue time."""
    try:
        return json.loads(json.dumps(value))
    except (TypeError, ValueError) as error:
        raise TypeError(
            f'{what} must be JSON (pass ids, not model instances): {error}'
        ) from error


def digest(*parts) -> str:
    text = json.dumps(parts, sort_keys=True, default=str)
    return hashlib.sha256(text.encode()).hexdigest()


class QueueBackend(BaseTaskBackend):
    task_class = QueueTask

    supports_defer = True
    supports_async_task = True
    supports_get_result = True
    supports_priority = True

    # Whether enqueue() waits for the surrounding transaction by default
    enqueue_on_commit_default = False

    def __init__(self, alias, params):
        super().__init__(alias, params)
        if 'QUEUES' not in params:
            self.queues = set()  # any queue name is fine

        options = self.options
        self.default_tries = options.get('TRIES', 1)
        self.default_backoff = options.get('BACKOFF', 60)
        self.default_timeout = options.get('TIMEOUT', 600)
        # Extra seconds past its timeout before a running job counts as lost
        self.lost_grace = options.get('LOST_GRACE', 30)
        # A job held back by without_overlapping is looked at again after
        self.overlap_retry = options.get('OVERLAP_RETRY', 5)
        # How long successful jobs are kept (seconds); None keeps them
        self.keep_results = options.get('KEEP_RESULTS', 7 * DAY)
        self.enqueue_on_commit = options.get(
            'ENQUEUE_ON_COMMIT', self.enqueue_on_commit_default
        )

    def validate_task(self, task):
        super().validate_task(task)
        validate_options(task)

    # -- django.tasks API -------------------------------------------------
    def enqueue(self, task, args, kwargs):
        return self.enqueue_job(task, args, kwargs)[0]

    def get_result(self, result_id):
        return build_result(self.get_record(str(result_id)))

    # -- enqueueing -------------------------------------------------------
    def enqueue_job(self, task, args, kwargs, schedule=None):
        """Returns (task result, queued). `queued` is False when a unique
        task was dropped; the result is then the job already in the queue.
        `schedule` names the schedule entry doing the enqueue."""
        self.validate_task(task)
        record = self.new_record(task, args, kwargs)
        record.schedule = schedule

        alias = self._commit_alias()
        if alias:
            # the worker must not run ahead of the data the job was queued
            # for, and a rolled back request must queue nothing
            transaction.on_commit(lambda: self._store(record, task), using=alias)
            return build_result(record, task), True

        return self._store(record, task)

    def new_record(self, task, args, kwargs) -> JobRecord:
        args = to_json(list(args), 'Task arguments')
        kwargs = to_json(dict(kwargs), 'Task arguments')
        now = timezone.now()

        tries = getattr(task, 'tries', None) or self.default_tries
        timeout = getattr(task, 'timeout', None) or self.default_timeout
        backoff = getattr(task, 'backoff', None)
        if backoff is None:
            backoff = self.default_backoff
        if not isinstance(backoff, (list, tuple)):
            backoff = [backoff]

        # multi-tenancy (off unless B_QUEUE['CONTEXT'] is set): the task's
        # own setting wins, otherwise ask the provider
        context = getattr(task, 'context', None)
        if context is None:
            context = capture()
        elif context is False:
            context = None
        if context is not None:
            context = to_json(context, 'Job context')

        unique_key = self._rule_key(
            'unique', task, getattr(task, 'unique', False), args, kwargs, context
        )
        overlap_key = self._rule_key(
            'overlap',
            task,
            getattr(task, 'without_overlapping', False),
            args,
            kwargs,
            context,
        )

        return JobRecord(
            id=str(uuid.uuid4()),
            task_path=task.module_path,
            queue_name=task.queue_name,
            priority=task.priority,
            args=args,
            kwargs=kwargs,
            backend=self.alias,
            enqueued_at=now,
            available_at=task.run_after or now,
            tries=tries,
            backoff=list(backoff),
            timeout=timeout,
            unique_key=unique_key,
            unique_until=getattr(task, 'unique_until', 'finish'),
            unique_for=getattr(task, 'unique_for', None),
            overlap_key=overlap_key,
            context=context,
        )

    def _rule_key(self, kind, task, rule, args, kwargs, context) -> str | None:
        if not rule:
            return None
        if callable(rule):
            key = rule(*args, **kwargs)
        elif rule is True:
            # unique: the same arguments. overlap: the task as a whole
            key = [args, kwargs] if kind == 'unique' else None
        else:
            key = rule
        if context is None:
            return digest(kind, task.module_path, key)
        # "unique" and "one at a time" mean per context (per tenant)
        return digest(kind, task.module_path, key, context)

    def _commit_alias(self) -> str | None:
        """The database whose open transaction the enqueue should wait for."""
        alias = self.enqueue_on_commit
        if isinstance(alias, str) and '.' in alias:
            alias = import_string(alias)  # dotted path of a function
        if callable(alias):
            # e.g. the current tenant's database; None: do not wait
            alias = alias()
        if not alias:
            return None
        if alias is True:
            alias = 'default'
        return alias if connections[alias].in_atomic_block else None

    def _store(self, record, task):
        holder = self.store(record)
        if holder is not None:
            return build_result(holder), False

        result = build_result(record, task)
        task_enqueued.send(sender=type(self), task_result=result)
        return result, True

    # -- helpers for subclasses -------------------------------------------
    @staticmethod
    def unique_expired(holder: JobRecord, now: datetime) -> bool:
        if holder.unique_for is None:
            return False
        return (now - holder.enqueued_at).total_seconds() > holder.unique_for

    def lost_outcome(self, record: JobRecord, now: datetime) -> bool:
        """Update a record whose worker vanished. True: it goes back on the
        queue; False: it is failed for good."""
        record.add_error(
            'b_queue.exceptions.JobTimedOut',
            f'Still running {record.timeout}s after it started: the worker '
            'was stopped mid-job or the job hung.',
        )
        record.timeout_at = None
        if record.attempts < record.tries:
            record.status = 'READY'
            record.available_at = now
            return True
        record.status = 'FAILED'
        record.finished_at = now
        return False

    # -- storage: implemented by the subclasses ---------------------------
    def store(self, record: JobRecord) -> JobRecord | None:
        """Save a new job. Returns None, or the unfinished job holding the
        same unique key (nothing is saved then)."""
        raise NotImplementedError

    def get_record(self, job_id: str) -> JobRecord:
        """Raises TaskResultDoesNotExist."""
        raise NotImplementedError

    def reserve(self, queues, worker_id: str, now=None) -> JobRecord | None:
        """Atomically take the next ready job and mark it running. `queues`
        is a list in priority order; ['*'] means every queue."""
        raise NotImplementedError

    def save(self, record: JobRecord) -> bool:
        """Write the state of a job this worker reserved: finished, or back
        to ready. Only if the reservation (record.token, set by reserve())
        still stands; False when the job was taken over meanwhile."""
        raise NotImplementedError

    def recover_lost(self, now=None) -> int:
        """Requeue or fail the jobs stuck in 'running' past their timeout."""
        raise NotImplementedError

    def acquire_lock(self, key: str, owner: str, ttl: float) -> bool:
        raise NotImplementedError

    def release_lock(self, key: str, owner: str) -> None:
        raise NotImplementedError

    def schedule_claim(self, name: str, due: datetime, now: datetime) -> bool:
        """True for exactly one caller per (entry, slot)."""
        raise NotImplementedError

    def schedule_forget(self, keep: list[str]) -> None:
        """Drop the state of entries that left the schedule."""
        raise NotImplementedError

    def failed_jobs(self) -> list[JobRecord]:
        raise NotImplementedError

    def retry_job(self, job_id: str) -> bool:
        """Put a failed job back on the queue with a fresh set of tries."""
        raise NotImplementedError

    def forget_job(self, job_id: str) -> bool:
        raise NotImplementedError

    def flush_failed(self) -> int:
        raise NotImplementedError

    def prune(self, before: datetime, include_failed: bool = False) -> int:
        """Delete finished jobs older than `before`."""
        raise NotImplementedError

    def clear(self, queue: str | None = None) -> int:
        """Delete the jobs still waiting to run."""
        raise NotImplementedError

    def stats(self) -> dict:
        """{'ready', 'delayed', 'running', 'failed', 'queues': {name: ready}}"""
        raise NotImplementedError


def get_queue_backend(alias: str = 'default') -> QueueBackend:
    from django.tasks import task_backends

    from b_queue.exceptions import QueueError

    backend = task_backends[alias]
    if not isinstance(backend, QueueBackend):
        raise QueueError(
            f"TASKS['{alias}'] uses {type(backend).__name__}, not a b_queue backend"
        )
    return backend
