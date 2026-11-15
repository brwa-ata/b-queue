"""QueueTask: Django's Task plus retry, timeout, unique and overlap rules.

It is the `task_class` of the b_queue backends, so Django's own decorator
already builds one:

    from django.tasks import task

    @task
    def send_email(user_id): ...

    @task(tries=3, backoff=[10, 60], unique=True)
    def recalculate_balance(customer_id): ...

`from b_queue import task` is the same decorator with the options spelled out
(editor completion) and it keeps working when a test swaps the backend for
Django's ImmediateBackend.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Any

from django.tasks import (
    DEFAULT_TASK_BACKEND_ALIAS,
    DEFAULT_TASK_QUEUE_NAME,
    Task,
)
from django.utils import timezone

from b_queue.exceptions import QueueError


@dataclass(frozen=True, slots=True, kw_only=True)
class QueueTask(Task):
    tries: int | None = None
    """Total runs allowed before the job stays failed (backend default: 1)"""

    backoff: Any = None
    """Seconds before a retry; a list gives one wait per retry"""

    timeout: int | None = None
    """Seconds one run may take"""

    unique: Any = False
    """True (same arguments), a function of the arguments returning a key, or
    a fixed key: an enqueue is dropped while such a job is still unfinished"""

    unique_until: str = 'finish'
    """'finish', or 'start' to stop blocking once a worker picks the job up"""

    unique_for: int | None = None
    """Seconds after which a unique job stops blocking no matter what"""

    without_overlapping: Any = False
    """True (the whole task), a function of the arguments returning a key, or
    a fixed key: two such jobs never run at the same time"""

    context: Any = None
    """Only with a context provider (multi-tenancy). None: capture the
    current context at enqueue. False: never (a task that is not tied to a
    tenant). A dict, set by with_context(): use exactly this"""

    # -- per-dispatch options, each returns a copy ------------------------
    def delay(
        self,
        seconds: float | timedelta | datetime = 0,
        *,
        minutes: float = 0,
        hours: float = 0,
        days: float = 0,
    ) -> QueueTask:
        """Not before this long from now: `.delay(minutes=10).enqueue()`.
        Also takes a timedelta, or a datetime to run at."""
        if isinstance(seconds, datetime):
            return self.using(run_after=seconds)
        if isinstance(seconds, timedelta):
            wait = seconds
        else:
            wait = timedelta(seconds=seconds, minutes=minutes, hours=hours, days=days)
        return self.using(run_after=timezone.now() + wait)

    def on_queue(self, queue_name: str) -> QueueTask:
        return self.using(queue_name=queue_name)

    def with_priority(self, priority: int) -> QueueTask:
        """-100 to 100; higher runs first."""
        return self.using(priority=priority)

    def with_context(self, **values) -> QueueTask:
        """Queue for an explicit context instead of the current one:
        `send_email.with_context(tenant=5).enqueue(...)`."""
        return replace(self, context=values)

    def without_context(self) -> QueueTask:
        return replace(self, context=False)

    # -- running ----------------------------------------------------------
    def dispatch(self, *args, **kwargs):
        """Same as enqueue(), for Laravel muscle memory."""
        return self.enqueue(*args, **kwargs)

    def dispatch_sync(self, *args, **kwargs):
        """Run now in this process, without the queue."""
        return self.call(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        return self.call(*args, **kwargs)

    # -- pickling ---------------------------------------------------------
    def __reduce__(self):
        # Task's own version rebuilds the dataclass from its fields, which
        # fails when `unique` is a lambda. Import the task back instead and
        # re-apply what can differ per dispatch.
        options = {
            'priority': self.priority,
            'queue_name': self.queue_name,
            'backend': self.backend,
            'run_after': self.run_after,
            'context': self.context,
        }
        return (_rebuild, (self.module_path, options))


def _rebuild(module_path: str, options: dict) -> QueueTask:
    from b_queue.record import load_task

    return replace(load_task(module_path), **options)


def validate_options(task: Task) -> None:
    """Checks for the QueueTask fields; called from the backend's
    validate_task(), i.e. whenever a task is declared or copied."""
    tries = getattr(task, 'tries', None)
    if tries is not None and tries < 1:
        raise QueueError(f'{task.module_path}: tries must be 1 or more')

    timeout = getattr(task, 'timeout', None)
    if timeout is not None and timeout <= 0:
        raise QueueError(f'{task.module_path}: timeout must be above 0')

    if getattr(task, 'unique_until', 'finish') not in ('finish', 'start'):
        raise QueueError(f"{task.module_path}: unique_until is 'finish' or 'start'")


def task(
    function=None,
    *,
    tries: int | None = None,
    backoff: Any = None,
    timeout: int | None = None,
    unique: Any = False,
    unique_until: str = 'finish',
    unique_for: int | None = None,
    without_overlapping: Any = False,
    context: Any = None,
    queue: str = DEFAULT_TASK_QUEUE_NAME,
    priority: int = 0,
    backend: str = DEFAULT_TASK_BACKEND_ALIAS,
    takes_context: bool = False,
):
    """Turn a module-level function into a task: bare (`@task`) or with
    options (`@task(tries=3, backoff=[10, 60], timeout=120)`)."""

    def wrap(func) -> QueueTask:
        return QueueTask(
            func=func,
            tries=tries,
            backoff=backoff,
            timeout=timeout,
            unique=unique,
            unique_until=unique_until,
            unique_for=unique_for,
            without_overlapping=without_overlapping,
            context=context,
            queue_name=queue,
            priority=priority,
            backend=backend,
            takes_context=takes_context,
            run_after=None,  # required on Django 6.0
        )

    if function is not None:
        return wrap(function)
    return wrap
