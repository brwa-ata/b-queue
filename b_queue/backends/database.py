"""Jobs in the project's database.

    TASKS = {'default': {'BACKEND': 'b_queue.backends.DatabaseBackend'}}

The job row is written inside the caller's transaction, so by default a job
becomes visible to the workers exactly when that transaction commits and
disappears with it on rollback.
"""

from __future__ import annotations

from datetime import timedelta

from django.db import IntegrityError, connections, router, transaction
from django.tasks.exceptions import TaskResultDoesNotExist
from django.tasks.signals import task_finished
from django.utils import timezone

from b_queue.backends.base import QueueBackend
from b_queue.exceptions import QueueError
from b_queue.record import FAILED, READY, RUNNING, SUCCESSFUL, JobRecord, build_result


def to_record(job) -> JobRecord:
    options = job.options
    return JobRecord(
        id=str(job.id),
        task_path=job.task_path,
        queue_name=job.queue_name,
        priority=job.priority,
        args=job.args_kwargs.get('args', []),
        kwargs=job.args_kwargs.get('kwargs', {}),
        backend=job.backend_name,
        enqueued_at=job.enqueued_at,
        available_at=job.available_at,
        tries=options.get('tries', 1),
        backoff=options.get('backoff', []),
        timeout=options.get('timeout', 600),
        status=job.status,
        started_at=job.started_at,
        finished_at=job.finished_at,
        last_attempted_at=job.last_attempted_at,
        timeout_at=job.timeout_at,
        worker_ids=list(job.worker_ids),
        unique_key=job.unique_key,
        unique_until=options.get('unique_until', 'finish'),
        unique_for=options.get('unique_for'),
        overlap_key=options.get('overlap_key'),
        context=job.context,
        return_value=job.return_value,
        errors=list(job.errors),
        token=job.timeout_at,
    )


def state_fields(record: JobRecord) -> dict:
    """The columns that change after a job was queued."""
    return {
        'status': record.status,
        'available_at': record.available_at,
        'started_at': record.started_at,
        'finished_at': record.finished_at,
        'last_attempted_at': record.last_attempted_at,
        'timeout_at': record.timeout_at,
        'worker_ids': record.worker_ids,
        'unique_key': record.unique_key,
        'return_value': record.return_value,
        'errors': record.errors,
    }


class DatabaseBackend(QueueBackend):
    @property
    def db(self) -> str:
        from b_queue.models import Job

        return self.options.get('DATABASE') or router.db_for_write(Job)

    @property
    def jobs(self):
        from b_queue.models import Job

        return Job.objects.using(self.db)

    # -- enqueue / results ------------------------------------------------
    def store(self, record):
        for _ in range(3):
            try:
                # a savepoint: a duplicate key must not break the caller's
                # transaction
                with transaction.atomic(using=self.db):
                    self.jobs.create(
                        id=record.id,
                        task_path=record.task_path,
                        queue_name=record.queue_name,
                        priority=record.priority,
                        backend_name=record.backend,
                        args_kwargs={'args': record.args, 'kwargs': record.kwargs},
                        enqueued_at=record.enqueued_at,
                        context=record.context,
                        options={
                            'tries': record.tries,
                            'backoff': record.backoff,
                            'timeout': record.timeout,
                            'unique_until': record.unique_until,
                            'unique_for': record.unique_for,
                            'overlap_key': record.overlap_key,
                        },
                        **state_fields(record),
                    )
                return None
            except IntegrityError:
                if not record.unique_key:
                    raise

            holder = self.jobs.filter(unique_key=record.unique_key).first()
            if holder is None:
                continue  # it finished in between: try again

            holder = to_record(holder)
            if not self.unique_expired(holder, timezone.now()):
                return holder
            self.jobs.filter(id=holder.id).update(unique_key=None)

        raise QueueError(f'Could not queue {record.task_path}: unique key is contended')

    def get_record(self, job_id):
        from django.core.exceptions import ValidationError

        try:
            job = self.jobs.filter(id=job_id).first()
        except (ValueError, ValidationError):
            job = None
        if job is None:
            raise TaskResultDoesNotExist(job_id)
        return to_record(job)

    # -- worker -----------------------------------------------------------
    def reserve(self, queues, worker_id, now=None):
        for queue in queues:
            record = self._reserve(queue, worker_id, now or timezone.now())
            if record:
                return record
        return None

    def _reserve(self, queue, worker_id, now):
        ready = self.jobs.filter(status=READY, available_at__lte=now)
        if queue != '*':
            ready = ready.filter(queue_name=queue)
        ready = ready.order_by('-priority', 'available_at', 'enqueued_at')

        if connections[self.db].features.has_select_for_update_skip_locked:
            # workers never wait on, or fight over, the same row
            with transaction.atomic(using=self.db):
                return self._claim(
                    ready.select_for_update(skip_locked=True)[:1], worker_id
                )

        # No row locks (SQLite): no transaction either, so that the claim is
        # one plain UPDATE that waits its turn instead of failing with
        # "database is locked".
        return self._claim(ready[:5], worker_id)

    def _claim(self, candidates, worker_id):
        for job in candidates:
            started = timezone.now()
            record = to_record(job)
            record.status = RUNNING
            record.started_at = record.last_attempted_at = started
            record.timeout_at = started + timedelta(
                seconds=record.timeout + self.lost_grace
            )
            record.worker_ids.append(worker_id)
            if record.unique_until == 'start':
                record.unique_key = None

            # the status check makes the claim atomic even without row locks
            claimed = self.jobs.filter(id=job.id, status=READY).update(
                **state_fields(record)
            )
            if claimed:
                record.token = record.timeout_at
                return record
        return None

    def save(self, record):
        if record.status in (FAILED, SUCCESSFUL):
            record.unique_key = None
        updated = self.jobs.filter(
            id=record.id, status=RUNNING, timeout_at=record.token
        ).update(**state_fields(record))
        return bool(updated)

    def recover_lost(self, now=None):
        now = now or timezone.now()
        recovered = 0

        for job in self.jobs.filter(status=RUNNING, timeout_at__lt=now):
            record = to_record(job)
            self.lost_outcome(record, now)
            if not self.save(record):
                continue  # its worker finished it just now
            recovered += 1
            if record.status == FAILED:
                self._send_finished(record)

        return recovered

    def _send_finished(self, record):
        try:
            result = build_result(record)
        except Exception:
            return  # the task's code is gone
        task_finished.send(sender=type(self), task_result=result)

    # -- locks ------------------------------------------------------------
    def acquire_lock(self, key, owner, ttl):
        from b_queue.models import Lock

        locks = Lock.objects.using(self.db)
        now = timezone.now()
        locks.filter(key=key, expires_at__lt=now).delete()
        try:
            with transaction.atomic(using=self.db):
                locks.create(
                    key=key, owner=owner, expires_at=now + timedelta(seconds=ttl)
                )
            return True
        except IntegrityError:
            return False

    def release_lock(self, key, owner):
        from b_queue.models import Lock

        Lock.objects.using(self.db).filter(key=key, owner=owner).delete()

    # -- scheduler state --------------------------------------------------
    def schedule_claim(self, name, due, now):
        from b_queue.models import ScheduleRun

        runs = ScheduleRun.objects.using(self.db)
        # one UPDATE (or one INSERT) decides the winner: no locks needed
        if runs.filter(name=name, last_due_at__lt=due).update(
            last_due_at=due, last_run_at=now
        ):
            return True
        if runs.filter(name=name).exists():
            return False
        try:
            with transaction.atomic(using=self.db):
                runs.create(name=name, last_due_at=due, last_run_at=now)
            return True
        except IntegrityError:
            return False

    def schedule_forget(self, keep):
        from b_queue.models import ScheduleRun

        ScheduleRun.objects.using(self.db).exclude(name__in=keep).delete()

    # -- management -------------------------------------------------------
    def failed_jobs(self):
        return [
            to_record(job)
            for job in self.jobs.filter(status=FAILED).order_by('finished_at')
        ]

    def retry_job(self, job_id):
        now = timezone.now()
        return bool(
            self.jobs.filter(id=job_id, status=FAILED).update(
                status=READY,
                available_at=now,
                finished_at=None,
                timeout_at=None,
                worker_ids=[],
            )
        )

    def forget_job(self, job_id):
        return bool(self.jobs.filter(id=job_id, status=FAILED).delete()[0])

    def flush_failed(self):
        return self.jobs.filter(status=FAILED).delete()[0]

    def prune(self, before, include_failed=False):
        statuses = [SUCCESSFUL, FAILED] if include_failed else [SUCCESSFUL]
        return self.jobs.filter(status__in=statuses, finished_at__lt=before).delete()[0]

    def clear(self, queue=None):
        ready = self.jobs.filter(status=READY)
        if queue:
            ready = ready.filter(queue_name=queue)
        return ready.delete()[0]

    def stats(self):
        from django.db.models import Count

        now = timezone.now()
        waiting = self.jobs.filter(status=READY)
        queues = dict(
            waiting.filter(available_at__lte=now)
            .values_list('queue_name')
            .annotate(Count('id'))
            .order_by('queue_name')
        )
        return {
            'ready': sum(queues.values()),
            'delayed': waiting.filter(available_at__gt=now).count(),
            'running': self.jobs.filter(status=RUNNING).count(),
            'failed': self.jobs.filter(status=FAILED).count(),
            'queues': queues,
        }
