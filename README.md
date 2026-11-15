# b-queue

Laravel-style queues and scheduled tasks for Django's built-in task framework
(`django.tasks`, Django 6+).

- A complete `django.tasks` backend with its own worker. Jobs live in your
  **database** or in **Redis**; switching is one line of settings.
- Retries with backoff, timeouts, delayed jobs, priorities, named queues,
  **unique jobs**, **without overlapping**, failed-job management.
- A **scheduler** declared in code: `schedule.task(send_email).daily_at('01:00')`.
- Optional **multi-tenancy**: jobs remember their tenant and run inside it.
- Safe by construction: a job is taken by exactly one worker, a schedule slot
  is queued exactly once however many servers run the scheduler, and jobs
  whose worker crashed are recovered.
- No dependencies besides Django (`redis` only if you use the Redis backend).

Requires Python 3.13+ and Django 6.0+.

## Install

The package is installed straight from GitHub (it is not on PyPI, so a bare
`pip install b-queue` / `uv add b-queue` will not find it).

With uv:

```bash
uv add "b-queue @ git+https://github.com/brwa-ata/b-queue"
```

With pip:

```bash
pip install "b-queue @ git+https://github.com/brwa-ata/b-queue"
```

For the Redis backend, add the `redis` extra:

```bash
uv add "b-queue[redis] @ git+https://github.com/brwa-ata/b-queue"
```

```bash
pip install "b-queue[redis] @ git+https://github.com/brwa-ata/b-queue"
```

To update to the latest version later:

```bash
uv lock --upgrade-package b-queue && uv sync
```

```bash
pip install --upgrade --force-reinstall "b-queue @ git+https://github.com/brwa-ata/b-queue"
```

To stay on a fixed version, append a tag, branch or commit to the URL, e.g.
`...brwa-ata/b-queue@21ca907`.

Then add the app and pick a backend:

```python
# settings.py
INSTALLED_APPS = [
    # ...
    'b_queue',
]

TASKS = {
    'default': {'BACKEND': 'b_queue.backends.DatabaseBackend'},
}
```

```bash
python manage.py migrate
python manage.py queue_work --schedule   # worker + scheduler in one process
```

That is the whole setup.

## Tasks

A task is a module-level function with Django's `@task` decorator. Put them
wherever you like (`tasks.py`, `tasks/customers.py`, ...).

```python
# customers/tasks.py
from django.tasks import task

@task
def send_email(customer_id):
    ...

@task(tries=3, backoff=[10, 60], timeout=120)
def calc_balance(customer_id):
    ...
```

Options need Django 6.1+ with `django.tasks.task`. `from b_queue import task`
is the same decorator with the options spelled out (editor completion); it
also works on Django 6.0 and keeps working when a test swaps the backend for
Django's `ImmediateBackend`.

### Dispatching

```python
from customers.tasks import send_email

def create(self, request):
    customer = ...
    send_email.enqueue(customer.id)                       # in the background
    send_email.dispatch(customer.id)                      # same thing
    send_email.delay(minutes=10).enqueue(customer.id)     # not before 10 minutes
    send_email.on_queue('emails').enqueue(customer.id)    # a named queue
    send_email.with_priority(10).enqueue(customer.id)     # -100..100, higher first
    send_email(customer.id)                               # right now, no queue
```

Arguments and return values are stored as JSON: pass ids, not model instances.

`enqueue()` returns Django's `TaskResult`:

```python
result = send_email.enqueue(customer.id)
result.id
# later
result = send_email.get_result(result_id)
result.status          # READY / RUNNING / SUCCESSFUL / FAILED
result.return_value
result.errors          # one entry (class path + traceback) per failed attempt
```

**Transactions.** With the database backend the job row is written inside your
transaction: workers see it exactly when you commit, and it vanishes on
rollback. With Redis an enqueue made inside `transaction.atomic()` is sent on
commit (`ENQUEUE_ON_COMMIT`, see [Redis](#redis)).

### Task options

| Option | Default | Meaning |
| --- | --- | --- |
| `tries` | `1` | Total runs allowed. Raise it only for work that is safe to run twice. |
| `backoff` | `60` | Seconds before a retry. A list gives one wait per retry: `[10, 60, 300]`. |
| `timeout` | `600` | Seconds one run may take. |
| `unique` | `False` | Drop an enqueue while the same job is unfinished. See below. |
| `unique_until` | `'finish'` | `'start'`: stop blocking once a worker picks the job up. |
| `unique_for` | `None` | Seconds after which a unique job stops blocking regardless. |
| `without_overlapping` | `False` | Never run two of these at the same time. See below. |
| `context` | `None` | Only with [multi-tenancy](#multi-tenancy-optional): `False` for a task tied to no tenant. |
| `queue_name` / `priority` | | Django's own (`queue=` in `b_queue.task`). |
| `takes_context` | `False` | Django's own: first argument is a `TaskContext` (`context.attempt`). |

### Retries and failures

A job that raises is retried while it has tries left, then stays **failed**
with every traceback recorded. Inside a task you can steer this:

```python
from b_queue import Fail, Retry

@task(tries=5, backoff=30)
def sync_prices(item_id):
    if rate_limited():
        raise Retry(delay=120)      # again in 2 minutes (counts as an attempt)
    if not item_exists(item_id):
        raise Fail('item deleted')  # fail now, skip the tries left
```

```bash
python manage.py queue_failed            # list (--trace shows tracebacks)
python manage.py queue_retry <id> | all  # back on the queue, fresh tries
python manage.py queue_forget <id>       # delete one
python manage.py queue_flush             # delete all failed jobs
```

To react to a job that failed for good, use Django's signal:

```python
from django.tasks import TaskResultStatus
from django.tasks.signals import task_finished

@receiver(task_finished)
def notify(sender, task_result, **kwargs):
    if task_result.status == TaskResultStatus.FAILED:
        ...
```

### Unique jobs

```python
@task(unique=True)                                   # same arguments
def recalculate(customer_id): ...

@task(unique=lambda customer_id, **_: customer_id)   # pick what matters
def recalculate(customer_id, reason=None): ...
```

While such a job is queued, running or waiting for a retry, enqueueing it
again does nothing and returns the `TaskResult` of the job already there.
Two requests enqueueing at the same instant still produce one job.

`unique_until='start'` is for recalculations: once a worker starts the job,
one more may queue up, so a change made mid-run is never missed.

### Without overlapping

```python
@task(without_overlapping=True)                            # one at a time
def rebuild_report(): ...

@task(without_overlapping=lambda account_id, **_: account_id)  # one per account
def post_entries(account_id): ...
```

Every job is still run, but never two with the same key at once: a job that
finds the lock taken goes back on the queue for a few seconds (that does not
count as an attempt). The lock expires with the job's timeout, so a crashed
worker cannot hold it forever.

## Scheduled tasks

Declare entries in a `schedules.py` module, in any installed app or next to
your `settings.py` / `urls.py`. They are found automatically.

```python
# schedules.py
from b_queue import schedule
from customers.tasks import send_email, calc_balance

schedule.task(send_email).daily_at('01:00').name('Send Email')
schedule.task(calc_balance, 42).every_five_minutes().without_overlapping()
schedule.command('clearsessions').weekly_on(5, '03:00')
schedule.task(send_email).cron('*/15 8-18 * * 1-5').name('Office hours email')
```

`schedule.task(task, *args, **kwargs)` schedules a task,
`schedule.command(name, *args, **options)` a management command. A due entry
is put on the queue; a worker runs it.

**Frequencies:** `every_minute()`, `every_two_minutes()` … `every_five_minutes()`,
`every_ten_minutes()`, `every_fifteen_minutes()`, `every_thirty_minutes()`,
`hourly()`, `hourly_at(15)`, `every_two_hours()` … `every_six_hours()`,
`daily()`, `daily_at('13:00')`, `twice_daily(1, 13)`, `weekly()`,
`weekly_on(1, '08:00')`, `monthly()`, `monthly_on(15, '09:30')`, `quarterly()`,
`yearly()`, `yearly_on(6, 1, '12:00')`, `cron('* * * * *')`.

**Constraints:** `weekdays()`, `weekends()`, `mondays()` … `sundays()`,
`days(1, 3, 5)` (0 = Sunday), `between('08:00', '17:00')`,
`unless_between('23:00', '04:00')`, `when(callable)`, `skip(callable)`,
`timezone('Asia/Baghdad')`.

**Options:** `name('...')`, `on_queue('reports')`,
`without_overlapping(expires_after=1440)`, `for_each(tenants)`
([multi-tenancy](#multi-tenancy-optional)).

- Times are in `settings.TIME_ZONE` unless the entry sets `.timezone(...)`.
- Two entries for the same task need a `.name(...)` each.
- `without_overlapping()` skips a slot while the previous run is unfinished
  (queued, running or retrying); `expires_after` is in minutes.
- Each slot is queued once, even with several servers running the scheduler.
- A slot missed by less than 5 minutes (a restart, a slow cron) still runs.
  Older slots are skipped, not run late, so a server coming back after hours
  of downtime does not fire everything at once, and a new `daily_at('01:00')`
  entry deployed at noon waits for the next 01:00.

```bash
python manage.py schedule_list   # entries and their next run
```

## Running it

### Development

```bash
python manage.py queue_work --schedule
```

One process: runs jobs and checks the schedule every minute.

### Server setup

On the server you run one extra long-lived process next to your web server:
the worker. Started with `--schedule` it is also the scheduler, so no cron
entry is needed.

**1. Install and migrate.** Same package, same `INSTALLED_APPS` and `TASKS`
settings as locally, then:

```bash
python manage.py migrate
```

(With one database per tenant: `python manage.py migrate b_queue --database control`.)

**2. Run the worker as a service.** With systemd:

```ini
# /etc/systemd/system/myproject-worker.service
[Unit]
Description=myproject queue worker
After=network.target

[Service]
WorkingDirectory=/var/www/myproject
ExecStart=/var/www/myproject/.venv/bin/python manage.py queue_work --schedule --processes 2
Restart=always
KillSignal=SIGTERM
TimeoutStopSec=630
User=www-data

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now myproject-worker
```

Or with supervisor:

```ini
# /etc/supervisor/conf.d/myproject-worker.conf
[program:myproject-worker]
command=/var/www/myproject/.venv/bin/python manage.py queue_work --schedule --processes 2
directory=/var/www/myproject
user=www-data
autostart=true
autorestart=true
stopsignal=TERM
stopwaitsecs=630
```

- `--processes 2` runs two jobs in parallel; raise it if jobs queue up.
- `TimeoutStopSec` / `stopwaitsecs` is how long a stop waits for the job in
  progress. Keep it above your longest task timeout (default 600 seconds).
- Use the same user and environment variables as your web service, so the
  worker reads the same settings.

**3. Restart the worker on every deploy.** A worker loads your code (tasks and
`schedules.py`) once, at start. Add this to your deploy script, next to the
web server restart:

```bash
sudo systemctl restart myproject-worker
```

The restart is safe: on SIGTERM a worker finishes the job in progress, then
exits. A schedule slot that comes due during the restart still runs once the
worker is back (within `SCHEDULE_GRACE`, 5 minutes by default).

**4. Check it.**

```bash
sudo systemctl status myproject-worker
journalctl -u myproject-worker -f        # one line per job: RUNNING / DONE / FAIL / RETRY
python manage.py schedule_list           # entries and their next run
python manage.py queue_status            # ready / delayed / running / failed
python manage.py queue_failed            # what failed, and why (--trace)
```

Things to watch:

- Schedule times follow `settings.TIME_ZONE`, not the server clock's zone.
- With the Redis backend, enable persistence (AOF) on the server's Redis so
  queued jobs survive a Redis restart.
- On several app servers you can run the same service on each: a job is taken
  by one worker only and every schedule slot is queued once.

### How `--schedule` works (and when to use cron instead)

`queue_work --schedule` starts a background thread in the worker. At the start
of every minute it works out, for each entry, the latest time slot of its cron
expression and asks the queue's storage whether that slot was queued already.
If not, it puts the task on the queue; the workers then run it like any other
job. The scheduler never runs a task itself, and the "was this slot queued"
check is one atomic write, which is why several schedulers never double up.
With `--processes N` only the first process runs this thread.

That is everything cron would do, so `--schedule` is the recommended setup:
one service, and worker and scheduler always restart together on the same
code.

Use cron instead only when your workers are not always running (started on
demand, or with `--stop-when-empty`): cron then keeps queueing entries on
time and the worker drains them when it runs. Drop `--schedule` and add:

```cron
* * * * * cd /var/www/myproject && .venv/bin/python manage.py schedule_run
```

`schedule_run` does one check and exits; `schedule_work` is the same loop as
`--schedule` as a process of its own. Running the scheduler in more than one
place is harmless.

`queue_work` options:

| Option | Meaning |
| --- | --- |
| `--queue high,default` | Queues to work, in priority order. Default: all. |
| `--schedule` | Also run the scheduler. |
| `--processes N` | N worker processes, restarted if one dies. |
| `--sleep 1` | Seconds to wait when the queue is empty. |
| `--max-jobs N` / `--max-time S` | Exit after N jobs / S seconds (let the process manager restart it; frees memory). |
| `--stop-when-empty` | Exit when nothing is left. |
| `--backend alias` | Which `TASKS` entry. |
| `--quiet` | No per-job output. |

Other commands: `queue_status`, `queue_failed`, `queue_retry`, `queue_forget`,
`queue_flush`, `queue_clear [--queue name]`, `queue_prune [--hours 168] [--failed]`,
`schedule_run`, `schedule_work`, `schedule_list`.

Logs go to the `b_queue` logger (retries, recovered jobs) and to Django's
`django.tasks` logger (job started / finished / failed with traceback).

## Redis

```python
TASKS = {
    'default': {
        'BACKEND': 'b_queue.backends.RedisBackend',
        'OPTIONS': {'URL': 'redis://localhost:6379/0'},
    },
}
```

Everything above works the same; no table is used (schedule state and locks
live in Redis too). You can also run both: keep `default` on the database and
add a second alias for Redis, then pick per task with
`@task(backend='redis')` and run `queue_work --backend redis`.

Redis cannot join your database transaction, so an enqueue made inside
`transaction.atomic()` is sent when the transaction commits and dropped on
rollback. Set `'ENQUEUE_ON_COMMIT': False` to send immediately, or to a
database alias to follow that database's transactions.

Use a Redis with persistence (AOF) if queued jobs must survive a Redis
restart.

## Multi-tenancy (optional)

Off by default: without the setting below nothing is captured, stored or
changed, and you can skip this section.

A multi-tenant project needs two things: remember which tenant enqueued a job,
and be back inside that tenant when the worker runs it. You provide both in
one small class; b-queue does the rest.

```python
# settings.py
B_QUEUE = {'CONTEXT': 'myproject.tenancy.QueueContext'}
```

```python
# myproject/tenancy.py
from contextlib import contextmanager

class QueueContext:
    def capture(self):
        """At enqueue: what to remember with the job (JSON), or None."""
        tenant = get_current_tenant()
        return {'tenant': tenant.id} if tenant else None

    @contextmanager
    def activate(self, context):
        """In the worker: wraps the task's run."""
        with tenant_scope(context['tenant']):
            yield
```

Tasks and views stay exactly as they are: `send_email.enqueue(customer.id)`
inside a request is queued for, and later run inside, that request's tenant.

With a provider configured:

- **Unique and without-overlapping are per tenant.** One tenant's queued or
  running job never blocks another tenant's.
- **Explicit tenant** (shell, management command, another tenant):
  `send_email.with_context(tenant=5).enqueue(...)`.
- **Tasks that belong to no tenant:** `@task(context=False)`, or
  `task.without_context()` for one dispatch.
- **Scheduled tasks** fan out with `for_each`: one job per tenant, each run
  inside its tenant. `without_overlapping()` is then checked per tenant, so a
  slow tenant does not hold back the others.

  ```python
  def tenants():
      return [{'tenant': id} for id in Tenant.objects.filter(is_active=True).values_list('id', flat=True)]

  schedule.task(stock_backup).daily_at('01:00').for_each(tenants)
  ```

  The function is called every time the entry is due, so new tenants are
  picked up without a restart. An entry without `for_each` runs once, outside
  any tenant.
- A job whose tenant cannot be activated (deleted tenant) fails like any other
  error; `queue_failed` shows each job's context.

Complete examples for the two usual tenancy models follow. Replace the
tenant helpers (`get_current_tenant`, `tenant_scope`, ...) with your own.

### Example: shared database, `tenant_id` column

Every table has a `tenant_id` and your managers filter on the current tenant,
kept in a contextvar.

```python
# settings.py
INSTALLED_APPS = [..., 'b_queue']

TASKS = {
    'default': {'BACKEND': 'b_queue.backends.DatabaseBackend'},
}

B_QUEUE = {'CONTEXT': 'myproject.tenancy.QueueContext'}
```

```python
# myproject/tenancy.py
from contextlib import contextmanager
from contextvars import ContextVar

current_tenant_id = ContextVar('current_tenant_id', default=None)  # set by your middleware


class QueueContext:
    def capture(self):
        tenant_id = current_tenant_id.get()
        return {'tenant': tenant_id} if tenant_id is not None else None

    @contextmanager
    def activate(self, context):
        token = current_tenant_id.set(context['tenant'])
        try:
            yield
        finally:
            current_tenant_id.reset(token)
```

```python
# myproject/schedules.py
from b_queue import schedule
from customers.tasks import send_statements
from tenants.models import Tenant


def tenants():
    ids = Tenant.objects.filter(is_active=True).values_list('id', flat=True)
    return [{'tenant': tenant_id} for tenant_id in ids]


schedule.task(send_statements).daily_at('01:00').for_each(tenants)
```

That is all: the job row is in the same database as your data, so it commits
and rolls back with the request.

### Example: one database per tenant

A shared "control" database holds the tenant list; each tenant has its own
database, chosen by a router from the current tenant.

```python
# settings.py
INSTALLED_APPS = [..., 'b_queue']

TASKS = {
    'default': {
        'BACKEND': 'b_queue.backends.DatabaseBackend',
        'OPTIONS': {
            # the queue tables live in the control database
            'DATABASE': 'control',
            # the job row is in another database than the tenant's data, so
            # queue it only once the tenant's transaction has committed
            'ENQUEUE_ON_COMMIT': 'myproject.tenancy.current_tenant_database',
        },
    },
}

B_QUEUE = {'CONTEXT': 'myproject.tenancy.QueueContext'}
```

```python
# myproject/tenancy.py
from contextlib import contextmanager

from django.db import connections

from tenants.models import Tenant
from tenants.tenant_context import get_current_tenant, get_current_tenant_db
from tenants.utils import tenant_scope   # registers the tenant DB and sets the contextvar


class QueueContext:
    def capture(self):
        tenant = get_current_tenant()
        return {'tenant': tenant['id']} if tenant else None

    @contextmanager
    def activate(self, context):
        tenant = Tenant.objects.using('control').get(pk=context['tenant'])
        try:
            with tenant_scope(tenant):
                yield
        finally:
            # a worker serves every tenant: do not keep one open connection
            # per tenant database between jobs
            connection = connections[tenant.db_name]
            if not connection.in_atomic_block:
                connection.close()


def current_tenant_database():
    """For ENQUEUE_ON_COMMIT: the current tenant's database alias, or None
    outside a tenant (the job is then queued right away)."""
    return get_current_tenant_db()
```

```python
# myproject/schedules.py
from b_queue import schedule
from api.tasks import stock_backup
from tenants.models import Tenant


def tenants():
    ids = Tenant.objects.using('control').filter(is_active=True).values_list('id', flat=True)
    return [{'tenant': tenant_id} for tenant_id in ids]


schedule.task(stock_backup).daily_at('01:00').for_each(tenants)
schedule.command('clearsessions').weekly()          # no tenant: runs once
```

Make your database router send the `b_queue` app to the control database
(or rely on the `DATABASE` option above, which the backend uses for every
query) and create the tables there only:

```bash
python manage.py migrate b_queue --database control
```

In both models, tasks and views are written as usual:

```python
@task(unique=True, tries=3)
def recalculate_balance(partner_id): ...

recalculate_balance.enqueue(partner.id)                        # current tenant
recalculate_balance.with_context(tenant=5).enqueue(partner_id) # from a shell or command
```

## Django admin

With the database backend and `django.contrib.admin` installed, two read-only
pages appear under **Queue**. There is nothing to configure.

**Jobs** lists every job still stored: task, status, queue, the schedule entry
that queued it (if any), attempts and times. Filter by status, queue or
schedule entry; open a job to see its arguments, return value and the
traceback of each failed attempt. The "Retry the selected failed jobs" action
puts failed jobs back on the queue.

**Schedules** lists the entries declared in your `schedules.py` files:

| Column | Meaning |
|---|---|
| Name, Task, Cron | The entry as declared, with its arguments, queue and timezone. |
| Next run | The next time slot of the entry. |
| Last slot | The last slot the scheduler handled. Empty until the scheduler has run. |
| Last job | Status of the newest job the entry queued; links to the job. |
| Jobs | How many of its jobs are stored, and how many failed; links to the Jobs page filtered to this entry. |

Good to know:

- Entries cannot be edited in the admin; they live in code.
- "Last slot" can be newer than "Last job": a slot that was skipped
  (`when()`, `between()`, `without_overlapping()`, or found later than
  `SCHEDULE_GRACE`) queues nothing.
- History goes back as far as the stored jobs: successful jobs are pruned
  after `KEEP_RESULTS` (7 days by default).
- Staff users need the "view job" / "view schedule" permissions.
- With the Redis backend the jobs are not in the database, so use
  `queue_status`, `queue_failed` and `schedule_list` instead. The Schedules
  page still lists such entries, without job tracking.
- If the queue lives in another database (`OPTIONS['DATABASE']`), the pages
  read from that database.

Upgrading from an earlier install: run `python manage.py migrate` once; the
job table gained a `schedule` column.

## Settings

Per backend, in `TASKS[alias]['OPTIONS']`:

| Option | Default | Meaning |
| --- | --- | --- |
| `TRIES` | `1` | Default `tries` for tasks that do not set it. |
| `BACKOFF` | `60` | Default `backoff`. |
| `TIMEOUT` | `600` | Default `timeout`. |
| `KEEP_RESULTS` | `604800` | Seconds successful jobs are kept (workers prune them). `None` keeps them. Failed jobs are kept until you delete them. |
| `LOST_GRACE` | `30` | Seconds past its timeout before a running job counts as lost. |
| `OVERLAP_RETRY` | `5` | Seconds before a job held back by `without_overlapping` is looked at again. |
| `ENQUEUE_ON_COMMIT` | DB: `False`, Redis: `True` | `True` (the `default` database), a database alias, or the dotted path of a function returning one. See [Redis](#redis) and [Multi-tenancy](#multi-tenancy-optional). |
| `DATABASE` | router's choice | Database alias of the job table (database backend). |
| `URL`, `PREFIX` | `redis://localhost:6379/0`, `b_queue` | Redis backend. |

`TASKS[alias]['QUEUES']` restricts the allowed queue names; by default any
name is accepted.

Package-wide, optional:

```python
B_QUEUE = {
    'SCHEDULE_MODULES': None,     # e.g. ['myproject.schedules']; None = autodiscover
    'SCHEDULE_GRACE': 300,        # seconds a missed slot is still run
    'SCHEDULE_BACKEND': 'default',  # where "already ran this slot" is stored
    'CONTEXT': None,              # context provider, see Multi-tenancy
}
```

## Testing your project

Run queued jobs inside the test, without a worker:

```python
from b_queue.testing import run_jobs

def test_signup_sends_email(client, mailoutbox):
    client.post('/signup/', {...})
    assert run_jobs() == 1                      # number of runs
    assert len(mailoutbox) == 1

run_jobs(include_delayed=True)                  # also .delay()ed jobs and retries
```

Failures are recorded (and retried per the task's rules), not raised, exactly
as in production. Or skip the queue altogether in tests with Django's
`ImmediateBackend`, which runs a task the moment it is enqueued.

With the **Redis** backend, Django's `TestCase` wraps each test in a
transaction that never commits, so set `'ENQUEUE_ON_COMMIT': False` in your
test settings (or use `captureOnCommitCallbacks(execute=True)`).

## How the critical cases are handled

| Case | What happens |
| --- | --- |
| Two workers reach for the same job | One gets it. Database: `SELECT ... FOR UPDATE SKIP LOCKED` (PostgreSQL, MySQL 8+) or an atomic conditional `UPDATE` (SQLite). Redis: one Lua script. |
| A worker is killed mid-job (deploy, OOM, power) | After the job's timeout + `LOST_GRACE` it is found by any worker or scheduler: retried if it has tries left, otherwise failed with `JobTimedOut`. |
| A job hangs | It is interrupted at its timeout (`JobTimedOut`) and retried or failed like any error. |
| A slow job is given up on, then finishes anyway | Its late result is discarded; it cannot overwrite the retry's state. |
| The same unique job is enqueued twice at once | One job. A unique index (database) or an atomic script (Redis) decides. |
| The request that enqueued a job rolls back | The job is not queued. |
| Cron fires twice / several servers run the scheduler | Each slot is claimed atomically; the entry is queued once. |
| The server was down over a slot | Run if found within `SCHEDULE_GRACE`, else skipped. |
| The task's code was deleted while jobs were queued | Those jobs fail with the import error; the worker keeps going. |
| The database or Redis goes away | Workers log it and keep polling until it is back. |
| Deploy / restart | SIGTERM: the job in progress finishes, then the worker exits. |

### Good to know

- Delivery is **at least once**. With the default `tries=1` a job is never run
  a second time, even after a crash (it is marked failed instead). With
  `tries > 1` a job whose worker died mid-run runs again, so make such tasks
  safe to repeat.
- The in-job timeout uses `SIGALRM`: Linux/macOS, which is what `queue_work`
  runs on. On Windows a hung job is only caught by lost-job recovery. Keep
  `timeout` above the longest real run.
- The scheduler's resolution is one minute.
- SQLite is fine for development and small sites; use PostgreSQL/MySQL or
  Redis when several workers process a lot of jobs.

## Development

```bash
uv sync
uv run pytest                                   # SQLite + an in-memory fake Redis

# against real services
BQ_TEST_REDIS=redis://localhost:6379/15 uv run pytest
BQ_TEST_POSTGRES=postgres://postgres@localhost:5432/postgres uv run --with "psycopg[binary]" pytest
```

The concurrency tests (several threads racing for jobs, unique keys and
schedule slots) only run on PostgreSQL.

## License

MIT
