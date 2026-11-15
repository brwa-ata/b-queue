"""Package-wide settings: the optional `B_QUEUE` dict in settings.py.

Per-backend options (tries, timeout, Redis URL, ...) live in
TASKS[alias]['OPTIONS'] instead; see backends/base.py.
"""

from django.conf import settings

DEFAULTS = {
    # Modules that declare schedule entries. None: every installed app's
    # `schedules` module plus `<project package>.schedules`.
    'SCHEDULE_MODULES': None,
    # A slot missed by more than this many seconds (server was down) is
    # skipped instead of run late.
    'SCHEDULE_GRACE': 300,
    # TASKS alias whose storage remembers which slots were already run.
    'SCHEDULE_BACKEND': 'default',
    # Dotted path of a context provider (multi-tenancy); see context.py.
    # None: the feature is off.
    'CONTEXT': None,
}


def get(name: str):
    return getattr(settings, 'B_QUEUE', {}).get(name, DEFAULTS[name])
