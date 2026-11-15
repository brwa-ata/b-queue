import time

from django.core.management.base import BaseCommand
from django.db import close_old_connections

from b_queue.management.commands.schedule_run import run_once
from b_queue.schedule import schedule


class Command(BaseCommand):
    help = (
        'Run the scheduler in the foreground: checks the schedule at the '
        'start of every minute. Use it instead of a cron entry.'
    )

    def handle(self, *args, **options):
        schedule.load()
        self.stdout.write('Scheduler running, Ctrl+C to stop')
        try:
            while True:
                close_old_connections()
                try:
                    run_once(self.stdout.write)
                except Exception as error:
                    # e.g. the database is restarting: try again next minute
                    self.stderr.write(f'Schedule check failed: {error!r}')
                time.sleep(60 - time.time() % 60)
        except KeyboardInterrupt:
            self.stdout.write('Scheduler stopped')
