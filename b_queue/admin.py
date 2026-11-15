from django.contrib import admin, messages

from b_queue.models import Job


@admin.register(Job)
class JobAdmin(admin.ModelAdmin):
    """Read-only view of the database queue, with a retry action."""

    list_display = (
        'task_path',
        'status',
        'queue_name',
        'priority',
        'attempts',
        'enqueued_at',
        'available_at',
        'finished_at',
    )
    list_filter = ('status', 'queue_name')
    search_fields = ('task_path', 'id')
    ordering = ('-enqueued_at',)
    actions = ['retry']

    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    @admin.action(description='Retry the selected failed jobs')
    def retry(self, request, queryset):
        from django.tasks import task_backends

        from b_queue.backends.base import QueueBackend

        count = 0
        for job in queryset.filter(status='FAILED'):
            backend = task_backends[job.backend_name]
            if isinstance(backend, QueueBackend) and backend.retry_job(str(job.id)):
                count += 1
        self.message_user(request, f'{count} job(s) queued again', messages.SUCCESS)
