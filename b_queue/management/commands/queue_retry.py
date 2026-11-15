from django.core.management.base import CommandError

from b_queue.management.commands._base import QueueCommand


class Command(QueueCommand):
    help = 'Put failed jobs back on the queue: queue_retry <id> [<id> ...] or queue_retry all'

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument('ids', nargs='+', help='Job ids, or "all"')

    def handle(self, *args, **options):
        backend = self.backend(options)
        ids = options['ids']
        if ids == ['all']:
            ids = [record.id for record in backend.failed_jobs()]

        missing = [job_id for job_id in ids if not backend.retry_job(job_id)]
        self.stdout.write(f'{len(ids) - len(missing)} job(s) queued again')
        if missing:
            raise CommandError(f'Not a failed job: {", ".join(missing)}')
