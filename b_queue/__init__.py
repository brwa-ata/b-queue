"""b_queue: Laravel-style queues and scheduled tasks for django.tasks.

    from django.tasks import task          # or: from b_queue import task

    @task
    def send_email(user_id): ...

    send_email.enqueue(user.id)                        # in the background
    send_email.delay(minutes=10).enqueue(user.id)      # not before 10 min

    # schedules.py
    from b_queue import schedule
    schedule.task(send_email, 1).daily_at('01:00').name('Send Email')

Run `manage.py queue_work --schedule` and that is the whole setup.
See README.md.
"""

from b_queue.exceptions import Fail, JobTimedOut, QueueError, Retry
from b_queue.schedule import schedule
from b_queue.task import QueueTask, task

__all__ = [
    'Fail',
    'JobTimedOut',
    'QueueError',
    'QueueTask',
    'Retry',
    'schedule',
    'task',
]
