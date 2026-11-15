"""The database queue in the Django admin: jobs, and the schedule.

Both pages only show what is stored in the database; with the Redis backend
use the queue_status / queue_failed / schedule_list commands instead.
"""

from django.contrib import admin, messages
from django.core.exceptions import PermissionDenied
from django.db.models import Count, Max, Q
from django.tasks import task_backends
from django.template.response import TemplateResponse
from django.urls import reverse
from django.utils import timezone
from django.utils.http import urlencode

from b_queue import conf
from b_queue.backends.base import QueueBackend
from b_queue.backends.database import DatabaseBackend
from b_queue.exceptions import QueueError
from b_queue.models import Job, ScheduleRun
from b_queue.record import FAILED
from b_queue.schedule import schedule


def _database_backend(alias: str | None = None) -> DatabaseBackend | None:
    """The database backend behind `alias`; without one, the first database
    backend in TASKS."""
    if alias is not None:
        backend = task_backends[alias]
        return backend if isinstance(backend, DatabaseBackend) else None
    for backend in task_backends.all():
        if isinstance(backend, DatabaseBackend):
            return backend
    return None


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    """Read-only view of the database queue, with a retry action."""

    list_display = (
        'task_path',
        'status',
        'queue_name',
        'schedule',
        'attempts',
        'enqueued_at',
        'available_at',
        'finished_at',
    )
    list_filter = ('status', 'queue_name', 'schedule')
    search_fields = ('task_path', 'id', 'schedule')
    ordering = ('-enqueued_at',)
    actions = ['retry']

    def get_queryset(self, request):
        queryset = super().get_queryset(request)
        # the queue may live in another database (OPTIONS['DATABASE'])
        backend = _database_backend()
        return queryset.using(backend.db) if backend else queryset

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    @admin.action(description='Retry the selected failed jobs')
    def retry(self, request, queryset):
        count = 0
        for job in queryset.filter(status=FAILED):
            backend = task_backends[job.backend_name]
            if isinstance(backend, QueueBackend) and backend.retry_job(str(job.id)):
                count += 1
        self.message_user(request, f'{count} job(s) queued again', messages.SUCCESS)


@admin.register(ScheduleRun)
class ScheduleAdmin(admin.ModelAdmin):
    """The schedule entries declared in code, each with its next run and
    what became of the jobs it queued. Nothing can be edited here: entries
    are changed in schedules.py."""

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def changelist_view(self, request, extra_context=None):
        if not self.has_view_permission(request):
            raise PermissionDenied

        error = None
        try:
            entries = schedule_entries()
        except QueueError as problem:  # e.g. two entries with the same name
            entries, error = [], str(problem)

        context = {
            **self.admin_site.each_context(request),
            'title': 'Schedules',
            'opts': self.model._meta,
            'entries': entries,
            'error': error,
            'now': timezone.now(),
        }
        return TemplateResponse(request, 'admin/b_queue/schedules.html', context)


def schedule_entries(now=None) -> list[dict]:
    """One row per declared schedule entry, for the admin page."""
    now = now or timezone.now()
    events = schedule.load().events

    # the last slot the scheduler handled, per entry
    slots = {}
    state = _database_backend(conf.get('SCHEDULE_BACKEND'))
    if state is not None:
        slots = dict(
            ScheduleRun.objects.using(state.db).values_list('name', 'last_due_at')
        )

    jobs_url = reverse('admin:b_queue_job_changelist')
    summaries = {}  # backend alias -> {entry: counts}
    rows = []

    for event in events:
        task = event.task.using(queue_name=event._queue) if event._queue else event.task
        backend = task.get_backend()
        row = {
            'name': event.label,
            'task': event.task.module_path,
            'args': _arguments(event),
            'expression': event.expression,
            'timezone': event._timezone.key if event._timezone else None,
            'queue': task.queue_name,
            'without_overlapping': event._overlap_expires is not None,
            'for_each': event._for_each is not None,
            'next_due': event.next_due(now),
            'last_slot': slots.get(event.label),
            'tracked': isinstance(backend, DatabaseBackend),
        }

        if row['tracked']:
            if backend.alias not in summaries:
                summaries[backend.alias] = _summaries(backend)
            counts = summaries[backend.alias].get(event.label, {})
            last_job = None
            if counts:
                last_job = (
                    backend.jobs.filter(schedule=event.label)
                    .order_by('-enqueued_at')
                    .first()
                )
            query = {'schedule': event.label}
            failed = {**query, 'status__exact': FAILED}
            row.update(
                total=counts.get('total', 0),
                failed=counts.get('failed', 0),
                last_job=last_job,
                jobs_url=f'{jobs_url}?{urlencode(query)}',
                failed_url=f'{jobs_url}?{urlencode(failed)}',
            )
        rows.append(row)

    return rows


def _summaries(backend: DatabaseBackend) -> dict:
    """{entry name: {'total', 'failed', 'last_queued'}} of the jobs still in
    the table (finished ones are pruned after KEEP_RESULTS)."""
    counted = (
        backend.jobs.filter(schedule__isnull=False)
        .values('schedule')
        .annotate(
            total=Count('id'),
            failed=Count('id', filter=Q(status=FAILED)),
            last_queued=Max('enqueued_at'),
        )
        .order_by()
    )
    return {row['schedule']: row for row in counted}


def _arguments(event) -> str:
    parts = [repr(value) for value in event.args]
    parts += [f'{name}={value!r}' for name, value in event.kwargs.items()]
    return ', '.join(parts)
