"""Tasks that ship with b_queue."""

from django.core.management import call_command

from b_queue.task import task


@task
def run_command(name, *args, **options):
    """Run a management command; behind `schedule.command(...)`."""
    call_command(name, *args, **options)
