from b_queue.management.commands._base import QueueCommand


class Command(QueueCommand):
    help = 'Show how many jobs are waiting, delayed, running and failed.'

    def handle(self, *args, **options):
        stats = self.backend(options).stats()
        for name in ('ready', 'delayed', 'running', 'failed'):
            self.stdout.write(f'{name:<8} {stats[name]}')
        for queue, size in stats['queues'].items():
            self.stdout.write(f'  queue {queue}: {size} ready')
