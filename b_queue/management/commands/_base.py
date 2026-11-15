from django.core.management.base import BaseCommand, CommandError

from b_queue.backends.base import get_queue_backend
from b_queue.exceptions import QueueError


class QueueCommand(BaseCommand):
    """A command that works on one TASKS backend."""

    def add_arguments(self, parser):
        parser.add_argument(
            '--backend', default='default', help='TASKS alias (default: "default")'
        )

    def backend(self, options):
        try:
            return get_queue_backend(options['backend'])
        except QueueError as error:
            raise CommandError(str(error)) from None
