from b_queue.management.commands._base import QueueCommand


class Command(QueueCommand):
    help = 'Delete every failed job.'

    def handle(self, *args, **options):
        count = self.backend(options).flush_failed()
        self.stdout.write(f'{count} failed job(s) deleted')
