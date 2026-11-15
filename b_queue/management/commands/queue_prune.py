from datetime import timedelta

from django.utils import timezone

from b_queue.management.commands._base import QueueCommand


class Command(QueueCommand):
    help = (
        'Delete finished jobs older than --hours (default 168, a week). '
        'Workers already prune successful jobs by themselves (KEEP_RESULTS); '
        'use --failed to prune old failed jobs too.'
    )

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument('--hours', type=float, default=168)
        parser.add_argument('--failed', action='store_true')

    def handle(self, *args, **options):
        before = timezone.now() - timedelta(hours=options['hours'])
        count = self.backend(options).prune(before, include_failed=options['failed'])
        self.stdout.write(f'{count} job(s) pruned')
