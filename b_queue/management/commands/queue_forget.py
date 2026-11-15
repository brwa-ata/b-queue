from django.core.management.base import CommandError

from b_queue.management.commands._base import QueueCommand


class Command(QueueCommand):
    help = 'Delete failed jobs by id. queue_flush deletes all of them.'

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument('ids', nargs='+')

    def handle(self, *args, **options):
        backend = self.backend(options)
        missing = [
            job_id for job_id in options['ids'] if not backend.forget_job(job_id)
        ]
        self.stdout.write(f'{len(options["ids"]) - len(missing)} failed job(s) deleted')
        if missing:
            raise CommandError(f'Not a failed job: {", ".join(missing)}')
