"""A minimal tenant setup for the tests: the current tenant is a contextvar."""

from contextlib import contextmanager
from contextvars import ContextVar

current = ContextVar('current_tenant', default=None)


@contextmanager
def tenant(tenant_id):
    token = current.set(tenant_id)
    try:
        yield
    finally:
        current.reset(token)


class QueueContext:
    def capture(self):
        tenant_id = current.get()
        return {'tenant': tenant_id} if tenant_id is not None else None

    @contextmanager
    def activate(self, context):
        if context['tenant'] == 'deleted':
            raise LookupError('tenant does not exist')
        with tenant(context['tenant']):
            yield


def no_database():
    """An ENQUEUE_ON_COMMIT function: no transaction to wait for."""
    return None

