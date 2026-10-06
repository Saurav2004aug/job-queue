"""
End-to-end demo: enqueue a mix of jobs, run a 4-thread worker pool until
everything is finished, then show what happened.

Run from the project root:  python -m examples.demo
"""
import logging
import os
import tempfile
import time

from jobqueue import JobQueue, Worker
from examples.tasks import tasks


def main():
    logging.basicConfig(level=logging.WARNING, format="  %(message)s")
    db = os.path.join(tempfile.mkdtemp(), "demo.db")
    q = JobQueue(db, backoff_base=0.1)   # short backoff so the demo is quick

    for i in range(20):
        q.enqueue("send_email", {"to": f"user{i}@example.com"})
    for i in range(5):
        q.enqueue("resize_image", {"width": 1920, "height": 1080}, priority=10)  # jumps the line
    for i in range(10):
        q.enqueue("call_flaky_api", {"n": i}, max_attempts=5)
    q.enqueue("charge_card", {"card": "invalid", "amount": 999}, max_attempts=3)
    first = q.enqueue("send_email", {"to": "vip@example.com"}, idempotency_key="welcome-vip")
    again = q.enqueue("send_email", {"to": "vip@example.com"}, idempotency_key="welcome-vip")
    print(f"Enqueued {q.stats()['queued']} jobs (idempotent re-enqueue returned same id: {first == again})\n")

    print("Running 4 worker threads (failures are retried with exponential backoff):")
    started = time.perf_counter()
    worker = Worker(q, tasks, concurrency=4, poll_interval=0.02)
    worker.run_until_empty(timeout=30)

    print(f"\nFinished in {time.perf_counter() - started:.2f}s")
    print("Worker stats:", worker.stats)
    print("Queue stats: ", {k: v for k, v in q.stats().items() if v})
    for job in q.list(status="dead"):
        print(f"Dead-letter job {job.id} ({job.task}) after {job.attempts} attempts: "
              f"{job.last_error.splitlines()[0]}")


if __name__ == "__main__":
    main()
