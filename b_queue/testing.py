"""Test helper: run queued jobs in the test process, without a worker.

from b_queue.testing import run_jobs

def test_signup_sends_email(client):
    client.post('/signup/', {...})
    assert run_jobs() == 1
"""

from b_queue.backends.base import get_queue_backend
from b_queue.worker import Worker


def run_jobs(
    *,
    include_delayed: bool = False,
    queues: list[str] | None = None,
    max_jobs: int | None = None,
    backend: str = 'default',
) -> int:
    """Run the jobs that are ready, the way a worker would: a failure is
    recorded (and retried per the task's rules), not raised. Jobs queued by
    the jobs that run are run too. `include_delayed` also runs jobs whose
    time has not come yet (`.delay(...)`, a retry waiting out its backoff).
    Returns the number of runs."""
    worker = Worker(
        get_queue_backend(backend),
        queues=queues,
        stop_when_empty=True,
        max_jobs=max_jobs,
        include_delayed=include_delayed,
        worker_id='test-worker',
    )
    return worker.run()
