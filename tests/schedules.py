"""Found by autodiscovery in test_schedule_modules_are_discovered."""

from b_queue import schedule
from tests.tasks import add

schedule.task(add, 1, 2).daily_at('01:00').name('Nightly add')
