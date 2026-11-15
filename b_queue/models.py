"""Tables of the database backend (unused, but harmless, with Redis)."""

import uuid

from django.db import models
from django.tasks import TaskResultStatus


class Job(models.Model):
    """One queued job, from enqueue to its final state."""

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    task_path = models.CharField(max_length=255)
    queue_name = models.CharField(max_length=100, default='default')
    priority = models.SmallIntegerField(default=0)
    backend_name = models.CharField(max_length=100, default='default')
    args_kwargs = models.JSONField(default=dict)
    status = models.CharField(
        max_length=10,
        choices=TaskResultStatus.choices,
        default=TaskResultStatus.READY,
    )

    enqueued_at = models.DateTimeField()
    available_at = models.DateTimeField()
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    last_attempted_at = models.DateTimeField(null=True, blank=True)
    timeout_at = models.DateTimeField(null=True, blank=True)

    worker_ids = models.JSONField(default=list)
    # Held (not null) while the job blocks other enqueues with the same key.
    # NULLs never collide in a unique index, on every database.
    unique_key = models.CharField(max_length=64, null=True, blank=True, unique=True)
    # tries, backoff, timeout, unique_until, unique_for, overlap_key
    options = models.JSONField(default=dict)
    # set only when a context provider is configured (multi-tenancy)
    context = models.JSONField(null=True, blank=True)
    return_value = models.JSONField(null=True, blank=True)
    errors = models.JSONField(default=list)

    class Meta:
        db_table = 'b_queue_job'
        indexes = [
            # the worker's "next job" query
            models.Index(
                fields=['status', 'queue_name', '-priority', 'available_at'],
                name='b_queue_job_next_idx',
            ),
            models.Index(fields=['status', 'timeout_at'], name='b_queue_job_lost_idx'),
            models.Index(fields=['status', 'finished_at'], name='b_queue_job_done_idx'),
            models.Index(fields=['task_path'], name='b_queue_job_task_idx'),
        ]

    def __str__(self) -> str:
        return f'{self.task_path} [{self.status}]'

    @property
    def attempts(self) -> int:
        return len(self.worker_ids)


class ScheduleRun(models.Model):
    """The last time slot each schedule entry was queued for. This is what
    makes an entry run once per slot, however many schedulers are running."""

    name = models.CharField(max_length=255, unique=True)
    last_due_at = models.DateTimeField()
    last_run_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'b_queue_schedule_run'

    def __str__(self) -> str:
        return self.name


class Lock(models.Model):
    """A lock with an expiry, behind without_overlapping."""

    key = models.CharField(max_length=64, primary_key=True)
    owner = models.CharField(max_length=64)
    expires_at = models.DateTimeField()

    class Meta:
        db_table = 'b_queue_lock'

    def __str__(self) -> str:
        return self.key
