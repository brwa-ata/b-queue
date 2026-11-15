"""JobRecord: one queued job, in the shape both storages share.

The database backend maps it to a row, the Redis backend to a JSON blob; the
worker and everything else only ever sees a JobRecord.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, fields, replace
from datetime import datetime
from typing import Any

from django.tasks import Task, TaskResult, TaskResultStatus
from django.tasks.base import TaskError
from django.utils.module_loading import import_string

from b_queue.exceptions import QueueError

READY = TaskResultStatus.READY.value
RUNNING = TaskResultStatus.RUNNING.value
FAILED = TaskResultStatus.FAILED.value
SUCCESSFUL = TaskResultStatus.SUCCESSFUL.value

_DATETIME_FIELDS = (
    'enqueued_at',
    'available_at',
    'started_at',
    'finished_at',
    'last_attempted_at',
    'timeout_at',
)


@dataclass
class JobRecord:
    id: str
    task_path: str
    queue_name: str
    priority: int
    args: list
    kwargs: dict
    backend: str
    enqueued_at: datetime
    available_at: datetime  # not picked up by a worker before this
    tries: int
    backoff: list  # seconds to wait before each retry; the last one repeats
    timeout: int
    status: str = READY
    started_at: datetime | None = None
    finished_at: datetime | None = None
    last_attempted_at: datetime | None = None
    # While running: when the job counts as lost
    timeout_at: datetime | None = None
    worker_ids: list = field(default_factory=list)
    unique_key: str | None = None
    unique_until: str = 'finish'
    unique_for: int | None = None
    overlap_key: str | None = None
    # What the context provider captured at enqueue (e.g. the tenant)
    context: dict | None = None
    return_value: Any = None
    errors: list = field(default_factory=list)
    # Identifies the reservation made by reserve(); not stored with the job
    token: Any = None

    @property
    def attempts(self) -> int:
        return len(self.worker_ids)

    def backoff_for(self, attempt: int) -> float:
        """Seconds to wait before the retry that follows attempt `attempt`."""
        if not self.backoff:
            return 0
        return self.backoff[min(attempt, len(self.backoff)) - 1]

    def add_error(self, class_path: str, traceback: str) -> None:
        self.errors.append({'exception_class_path': class_path, 'traceback': traceback})

    # -- JSON (Redis) -----------------------------------------------------
    def to_json(self) -> str:
        data = asdict(self)
        data.pop('token')
        for name in _DATETIME_FIELDS:
            if data[name] is not None:
                data[name] = data[name].isoformat()
        return json.dumps(data)

    @classmethod
    def from_json(cls, raw: str | bytes) -> JobRecord:
        data = json.loads(raw)
        for name in _DATETIME_FIELDS:
            if data.get(name) is not None:
                data[name] = datetime.fromisoformat(data[name])
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})


def load_task(path: str) -> Task:
    """Import the task a job was queued for."""
    found = import_string(path)
    if not isinstance(found, Task):
        raise QueueError(f'{path!r} is not a task (is the @task decorator missing?)')
    return found


def build_result(record: JobRecord, task: Task | None = None) -> TaskResult:
    """The django.tasks TaskResult of a record."""
    task = task or load_task(record.task_path)
    changes = {
        'priority': record.priority,
        'queue_name': record.queue_name,
        'backend': record.backend,
    }
    if record.context is not None and hasattr(task, 'context'):
        changes['context'] = record.context
    task = replace(task, **changes)
    result = TaskResult(
        task=task,
        id=record.id,
        status=TaskResultStatus(record.status),
        enqueued_at=record.enqueued_at,
        started_at=record.started_at,
        finished_at=record.finished_at,
        last_attempted_at=record.last_attempted_at,
        args=record.args,
        kwargs=record.kwargs,
        backend=record.backend,
        errors=[TaskError(**error) for error in record.errors],
        worker_ids=list(record.worker_ids),
    )
    object.__setattr__(result, '_return_value', record.return_value)
    return result
