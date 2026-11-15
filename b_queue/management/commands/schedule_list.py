from django.core.management.base import BaseCommand
from django.utils import timezone

from b_queue.schedule import schedule


class Command(BaseCommand):
    help = 'List the schedule entries and when each runs next.'

    def handle(self, *args, **options):
        events = schedule.load().events
        if not events:
            self.stdout.write(
                'No schedule entries. Declare them in a schedules.py module.'
            )
            return

        now = timezone.now()
        width = max(len(event.label) for event in events)
        for event in events:
            self.stdout.write(
                f'{event.expression:<16} {event.label:<{width}}  '
                f'next: {event.next_due(now):%Y-%m-%d %H:%M}'
            )
