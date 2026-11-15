class QueueError(Exception):
    """Base class of every b_queue error."""


class JobTimedOut(QueueError):
    """The job ran longer than its timeout, or its worker died mid-job."""


class Fail(QueueError):
    """Raise inside a task to fail it now, without using the tries left."""


class Retry(QueueError):
    """Raise inside a task to run it again later (e.g. a rate-limited API).

    It counts as an attempt: when no tries are left the job fails. `delay`
    (seconds) replaces the task's backoff for this retry.
    """

    def __init__(self, message: str = '', *, delay: float | None = None):
        super().__init__(message)
        self.delay = delay
