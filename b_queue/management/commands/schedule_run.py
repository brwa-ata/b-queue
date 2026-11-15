from datetime import datetime

from django.core.management.base import BaseCommand
from django.tasks import task_backends

from b_queue.backends.base import QueueBackend
from b_queue.schedule import schedule


def run_once(write) -> None:
    for alias in task_backends:
        backend = task_backends[alias]
        if isinstance(backend, QueueBackend):
            recovered = backend.recover_lost()
            if recovered:
                write(f'  recovered {recovered} lost job(s)')

    for event, due in schedule.run():
        write(f'  queued  {event.label}  (slot {due:%Y-%m-%d %H:%M})')
    write(f'{datetime.now():%Y-%m-%d %H:%M:%S}  schedule checked')


class Command(BaseCommand):
    help = (
        'Queue every schedule entry that is due, then exit. Meant for cron, '
        'once a minute:  * * * * * cd /path/to/project && python manage.py schedule_run'
    )

    def handle(self, *args, **options):
        schedule.load()
        run_once(self.stdout.write)
