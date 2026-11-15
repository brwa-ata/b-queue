from b_queue.management.commands._base import QueueCommand


class Command(QueueCommand):
    help = 'Delete the jobs still waiting to run (running and failed jobs are kept).'

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument('--queue', help='Only this queue')

    def handle(self, *args, **options):
        count = self.backend(options).clear(options['queue'])
        self.stdout.write(f'{count} waiting job(s) deleted')
