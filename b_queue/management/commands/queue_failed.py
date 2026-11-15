from b_queue.management.commands._base import QueueCommand


class Command(QueueCommand):
    help = 'List the failed jobs. Retry one with queue_retry <id>.'

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            '--trace', action='store_true', help='Show the last traceback of each'
        )

    def handle(self, *args, **options):
        failed = self.backend(options).failed_jobs()
        if not failed:
            self.stdout.write('No failed jobs.')
            return

        for record in failed:
            error = record.errors[-1] if record.errors else {}
            self.stdout.write(
                f'{record.id}  {record.finished_at:%Y-%m-%d %H:%M:%S}  '
                f'{record.task_path}  queue={record.queue_name}  '
                f'attempts={record.attempts}  {error.get("exception_class_path", "")}'
                + (f'  context={record.context}' if record.context else '')
            )
            if options['trace'] and error:
                self.stdout.write(error['traceback'])
