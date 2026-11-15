"""Job context: optional, for multi-tenant projects (and anything else that
must travel from the code that enqueues a job to the worker that runs it).

Off unless the project names a provider:

    B_QUEUE = {'CONTEXT': 'myproject.tenancy.QueueContext'}

    class QueueContext:
        def capture(self):
            # At enqueue time: what to remember with the job (JSON), or None.
            return {'tenant': current_tenant_id()}

        @contextmanager
        def activate(self, context):
            # In the worker: wraps the task's run.
            with tenant_scope(context['tenant']):
                yield

With no provider nothing is captured, stored or activated.
"""

from __future__ import annotations

from contextlib import contextmanager

from django.utils.module_loading import import_string

from b_queue import conf

_providers = {}


def provider():
    path = conf.get('CONTEXT')
    if not path:
        return None
    if path not in _providers:
        found = import_string(path)
        _providers[path] = found() if isinstance(found, type) else found
    return _providers[path]


def capture() -> dict | None:
    """The context of the code running right now."""
    current = provider()
    return current.capture() if current else None


@contextmanager
def activate(context: dict | None):
    """Run a block inside a stored context. Does nothing for a job that
    carries none (it was queued outside any tenant, or the feature is off)."""
    current = provider()
    if current is None or context is None:
        yield
        return
    with current.activate(context):
        yield
