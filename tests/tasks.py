"""Tasks used by the tests. `calls` records what actually ran."""

import time

import django
from django.tasks import task as django_task

from b_queue import Fail, Retry, task

calls = []
failures_left = {'flaky': 0}


@task
def add(a, b):
    calls.append(('add', a, b))
    return a + b


# Django's own decorator: the backend's task class takes the options
@django_task
def plain(value):
    calls.append(('plain', value))
    return value


# Django 6.1+ passes extra options through; on 6.0 use b_queue's decorator
@(django_task if django.VERSION >= (6, 1) else task)(tries=2, backoff=0)
def plain_with_options():
    calls.append(('plain_with_options',))


@task(tries=3, backoff=[10, 20])
def flaky():
    calls.append(('flaky',))
    if failures_left['flaky'] > 0:
        failures_left['flaky'] -= 1
        raise ValueError('not yet')
    return 'ok'


@task
def boom():
    calls.append(('boom',))
    raise ValueError('boom')


@task(tries=5)
def give_up():
    calls.append(('give_up',))
    raise Fail('pointless to retry')


@task(tries=2)
def retry_later():
    calls.append(('retry_later',))
    raise Retry('rate limited', delay=5)


@task(unique=True)
def unique_job(value):
    calls.append(('unique_job', value))


@task(unique=lambda customer_id, **_: customer_id, unique_until='start')
def recalculate(customer_id, note=None):
    calls.append(('recalculate', customer_id, note))


@task(unique=True, unique_for=60)
def unique_for_a_minute():
    calls.append(('unique_for_a_minute',))


@task(without_overlapping=True)
def exclusive():
    calls.append(('exclusive',))


@task(timeout=1)
def slow(seconds):
    time.sleep(seconds)
    calls.append(('slow',))


@task(takes_context=True, tries=2, backoff=0)
def with_context(context):
    calls.append(('with_context', context.attempt))
    if context.attempt == 1:
        raise ValueError('first attempt fails')
    return context.attempt


@task
async def async_add(a, b):
    calls.append(('async_add', a, b))
    return a + b


@task
def chain(value):
    calls.append(('chain', value))
    if value > 0:
        chain.using(backend=chain_backend['alias']).enqueue(value - 1)


chain_backend = {'alias': 'default'}


@task
def whoami():
    from tests import tenancy

    calls.append(('whoami', tenancy.current.get()))
    return tenancy.current.get()


@task(context=False)
def global_job():
    from tests import tenancy

    calls.append(('global_job', tenancy.current.get()))
