"""
Worker pool: N threads that claim jobs, run the registered handler, and
report success or failure back to the queue.

  * Heartbeats: while a job runs, a background timer renews its lease every
    lease/3 seconds, so long jobs aren't mistaken for crashed ones.
  * Graceful shutdown: stop() lets in-flight jobs finish (no half-done work)
    and stops claiming new ones. Anything left is safe in the database.
  * Idle backoff: an empty queue is polled with growing intervals (capped)
    to avoid hammering the database.
"""

import logging
import threading
import time
import traceback
from typing import Callable, Optional

from .queue import DEAD, Job, JobQueue, default_worker_id

log = logging.getLogger("jobqueue")
log.addHandler(logging.NullHandler())  # library: stay silent unless the app configures logging


class TaskRegistry:
    """Maps task names to handler functions.

        tasks = TaskRegistry()

        @tasks.task("send_email")
        def send_email(payload):
            ...
    """

    def __init__(self):
        self.handlers: dict[str, Callable] = {}

    def task(self, name: Optional[str] = None):
        def register(fn):
            self.handlers[name or fn.__name__] = fn
            return fn
        return register

    def __contains__(self, name):
        return name in self.handlers


class Worker:
    def __init__(self, queue: JobQueue, registry: TaskRegistry, *, concurrency: int = 4,
                 queues: tuple[str, ...] = ("default",), poll_interval: float = 0.2,
                 max_poll_interval: float = 2.0, name: Optional[str] = None):
        self.queue = queue
        self.registry = registry
        self.concurrency = concurrency
        self.queues = queues
        self.poll_interval = poll_interval
        self.max_poll_interval = max_poll_interval
        self.name = name or default_worker_id()
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._lock = threading.Lock()
        self.stats = {"succeeded": 0, "retried": 0, "dead": 0}

    # ---- lifecycle ----

    def start(self) -> "Worker":
        for i in range(self.concurrency):
            t = threading.Thread(target=self._run, args=(f"{self.name}#{i}",),
                                 name=f"worker-{i}", daemon=True)
            t.start()
            self._threads.append(t)
        return self

    def stop(self, timeout: Optional[float] = None) -> None:
        """Stop claiming new jobs and wait for in-flight ones to finish."""
        self._stop.set()
        for t in self._threads:
            t.join(timeout)

    def run_until_empty(self, timeout: float = 60.0) -> None:
        """Start, wait until every job has finished (succeeded, dead or
        cancelled, including pending retries), then stop gracefully.
        Handy for tests, scripts and one-off batch runs."""
        self.start()
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            s = self.queue.stats()
            if s["queued"] == 0 and s["running"] == 0:
                break
            time.sleep(0.05)
        self.stop()

    # ---- main loop ----

    def _run(self, worker_id: str) -> None:
        idle = self.poll_interval
        try:
            while not self._stop.is_set():
                job = self.queue.claim(worker_id, self.queues)
                if job is None:
                    self._stop.wait(idle)
                    idle = min(self.max_poll_interval, idle * 2)  # back off while idle
                    continue
                idle = self.poll_interval
                self._process(job, worker_id)
        finally:
            self.queue.close()  # this thread's SQLite connection

    def _process(self, job: Job, worker_id: str) -> None:
        handler = self.registry.handlers.get(job.task)
        stop_heartbeat = threading.Event()
        hb = threading.Thread(target=self._heartbeat, args=(job.id, worker_id, stop_heartbeat), daemon=True)
        hb.start()
        started = time.perf_counter()
        try:
            if handler is None:
                raise LookupError(f"no handler registered for task {job.task!r}")
            result = handler(job.payload)
        except Exception as e:
            stop_heartbeat.set()
            hb.join()
            error = f"{type(e).__name__}: {e}\n{traceback.format_exc(limit=5)}"
            status = self.queue.fail(job.id, worker_id, error)
            with self._lock:
                self.stats["dead" if status == DEAD else "retried"] += 1
            log.warning("job %s (%s) attempt %s/%s failed -> %s: %s",
                        job.id, job.task, job.attempts, job.max_attempts, status, e)
        else:
            stop_heartbeat.set()
            hb.join()
            if self.queue.complete(job.id, worker_id, result):
                with self._lock:
                    self.stats["succeeded"] += 1
                log.info("job %s (%s) succeeded in %.3fs", job.id, job.task, time.perf_counter() - started)
            else:
                log.warning("job %s finished but lease was lost; result discarded", job.id)

    def _heartbeat(self, job_id: int, worker_id: str, stop: threading.Event) -> None:
        interval = self.queue.lease_seconds / 3
        try:
            while not stop.wait(interval):
                if not self.queue.heartbeat(job_id, worker_id):
                    log.warning("job %s: lease lost (another worker took over)", job_id)
                    return
        finally:
            self.queue.close()
