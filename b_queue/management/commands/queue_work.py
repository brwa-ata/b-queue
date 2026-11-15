import multiprocessing
import signal
import threading
import time

from django.core.management.base import CommandError

from b_queue.management.commands._base import QueueCommand
from b_queue.worker import Worker


def run_scheduler(stop: threading.Event, write) -> None:
    """Queue due schedule entries at the start of every minute."""
    from django.db import close_old_connections

    from b_queue.schedule import schedule

    schedule.load()
    while not stop.is_set():
        close_old_connections()
        try:
            for event, due in schedule.run():
                write(f'scheduled {event.label} (slot {due:%Y-%m-%d %H:%M})')
        except Exception as error:
            write(f'scheduler error: {error!r}')
        stop.wait(60 - time.time() % 60)


def run_worker(options: dict, write=print) -> int:
    """One worker in this process, until it is told to stop."""
    from b_queue.backends.base import get_queue_backend

    worker = Worker(
        get_queue_backend(options['backend']),
        queues=options['queues'],
        sleep=options['sleep'],
        stop_when_empty=options['stop_when_empty'],
        max_jobs=options['max_jobs'],
        max_time=options['max_time'],
        output=None if options['quiet'] else write,
    )

    # first signal: finish the current job, then exit. A second Ctrl+C
    # interrupts the job (it is retried or failed like any other error).
    def on_signal(signum, frame):
        if worker._stop.is_set() and signum == signal.SIGINT:
            raise KeyboardInterrupt
        worker.stop()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    if options['schedule']:
        threading.Thread(
            target=run_scheduler, args=(worker._stop, write), daemon=True
        ).start()

    try:
        return worker.run()
    except KeyboardInterrupt:
        return worker.processed


def _child(options: dict) -> None:
    import django

    django.setup()
    run_worker(options)


class Command(QueueCommand):
    help = (
        'Run queued tasks. Stops cleanly on SIGTERM / Ctrl+C: the job in '
        'progress is finished first.'
    )

    def add_arguments(self, parser):
        super().add_arguments(parser)
        parser.add_argument(
            '--queue',
            default='*',
            help='Queues to work, in priority order: "high,default". Default: all',
        )
        parser.add_argument(
            '--schedule',
            action='store_true',
            help='Also run the scheduler here (no cron entry needed)',
        )
        parser.add_argument(
            '--processes', type=int, default=1, help='Number of worker processes'
        )
        parser.add_argument(
            '--sleep',
            type=float,
            default=1.0,
            help='Seconds to wait when the queue is empty (default: 1)',
        )
        parser.add_argument(
            '--stop-when-empty',
            action='store_true',
            help='Exit once there is nothing left to run',
        )
        parser.add_argument(
            '--max-jobs', type=int, help='Exit after this many jobs (frees memory)'
        )
        parser.add_argument(
            '--max-time', type=float, help='Exit after this many seconds'
        )
        parser.add_argument('--quiet', action='store_true', help='No per-job output')

    def handle(self, *args, **options):
        self.backend(options)  # fail early on a wrong alias

        settings = {
            'backend': options['backend'],
            'queues': [
                name.strip() for name in options['queue'].split(',') if name.strip()
            ],
            'sleep': options['sleep'],
            'stop_when_empty': options['stop_when_empty'],
            'max_jobs': options['max_jobs'],
            'max_time': options['max_time'],
            'quiet': options['quiet'],
            'schedule': options['schedule'],
        }
        if options['processes'] < 1:
            raise CommandError('--processes must be 1 or more')

        if options['processes'] == 1:
            self.stdout.write(
                f'Worker started on queues: {", ".join(settings["queues"])}'
            )
            done = run_worker(settings, self.stdout.write)
            self.stdout.write(f'Worker stopped after {done} job(s)')
            return

        self.supervise(settings, options['processes'])

    def supervise(self, settings: dict, count: int) -> None:
        """Keep `count` worker processes alive until told to stop."""
        context = multiprocessing.get_context('spawn')
        stopping = threading.Event()

        def start(index: int):
            # only one process runs the scheduler
            child = dict(settings, schedule=settings['schedule'] and index == 0)
            process = context.Process(target=_child, args=(child,), daemon=False)
            process.start()
            return process

        def on_signal(signum, frame):
            stopping.set()

        signal.signal(signal.SIGTERM, on_signal)
        signal.signal(signal.SIGINT, on_signal)

        workers = [start(index) for index in range(count)]
        self.stdout.write(f'{count} workers started')

        restart = not (
            settings['stop_when_empty'] or settings['max_jobs'] or settings['max_time']
        )
        while not stopping.is_set():
            for index, process in enumerate(workers):
                if not process.is_alive() and restart:
                    self.stderr.write(f'Worker {index} exited, starting a new one')
                    workers[index] = start(index)
            if not restart and not any(process.is_alive() for process in workers):
                break
            stopping.wait(1)

        for process in workers:
            if process.is_alive():
                process.terminate()  # SIGTERM: finish the current job
        for process in workers:
            process.join()
        self.stdout.write('Workers stopped')
