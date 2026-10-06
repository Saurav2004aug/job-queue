"""
Command-line interface.

    python -m jobqueue enqueue send_email '{"to": "a@b.com"}' --priority 5
    python -m jobqueue worker --tasks examples.tasks --concurrency 4
    python -m jobqueue stats
    python -m jobqueue list --status dead
    python -m jobqueue requeue 42
"""

import argparse
import importlib
import json
import logging
import signal
import sys
import threading

from .queue import JobQueue
from .worker import TaskRegistry, Worker


def load_registry(module_path: str) -> TaskRegistry:
    module = importlib.import_module(module_path)
    for value in vars(module).values():
        if isinstance(value, TaskRegistry):
            return value
    raise SystemExit(f"no TaskRegistry found in module {module_path!r}")


def main(argv=None):
    p = argparse.ArgumentParser(prog="jobqueue")
    p.add_argument("--db", default="jobs.db", help="SQLite database file (default: jobs.db)")
    sub = p.add_subparsers(dest="cmd", required=True)

    e = sub.add_parser("enqueue", help="add a job")
    e.add_argument("task")
    e.add_argument("payload", nargs="?", default="null", help="JSON payload")
    e.add_argument("--queue", default="default")
    e.add_argument("--priority", type=int, default=0)
    e.add_argument("--delay", type=float, default=0.0, help="seconds before the job may run")
    e.add_argument("--max-attempts", type=int, default=3)
    e.add_argument("--key", help="idempotency key")

    w = sub.add_parser("worker", help="run a worker pool")
    w.add_argument("--tasks", required=True, help="module containing a TaskRegistry, e.g. examples.tasks")
    w.add_argument("--concurrency", type=int, default=4)
    w.add_argument("--queues", default="default", help="comma-separated queue names")

    sub.add_parser("stats", help="job counts by status")
    ls = sub.add_parser("list", help="list recent jobs")
    ls.add_argument("--status")
    ls.add_argument("--limit", type=int, default=20)
    rq = sub.add_parser("requeue", help="retry a dead job")
    rq.add_argument("job_id", type=int)
    c = sub.add_parser("cancel", help="cancel a queued job")
    c.add_argument("job_id", type=int)

    args = p.parse_args(argv)
    q = JobQueue(args.db)

    if args.cmd == "enqueue":
        job_id = q.enqueue(args.task, json.loads(args.payload), queue=args.queue,
                           priority=args.priority, delay=args.delay,
                           max_attempts=args.max_attempts, idempotency_key=args.key)
        print(job_id)

    elif args.cmd == "worker":
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(threadName)s %(message)s")
        sys.path.insert(0, ".")
        worker = Worker(q, load_registry(args.tasks), concurrency=args.concurrency,
                        queues=tuple(args.queues.split(",")))
        stop = threading.Event()
        signal.signal(signal.SIGINT, lambda *_: stop.set())
        signal.signal(signal.SIGTERM, lambda *_: stop.set())
        worker.start()
        print(f"worker {worker.name} started with {args.concurrency} threads (Ctrl+C to stop)", flush=True)
        stop.wait()
        print("stopping: finishing in-flight jobs...", flush=True)
        worker.stop()
        print(f"stopped. {worker.stats}")

    elif args.cmd == "stats":
        print(json.dumps(q.stats(), indent=2))

    elif args.cmd == "list":
        for j in q.list(args.status, args.limit):
            err = (j.last_error or "").splitlines()[0] if j.last_error else ""
            print(f"{j.id:>6}  {j.status:<10} {j.task:<20} attempts={j.attempts}/{j.max_attempts}  {err}")

    elif args.cmd == "requeue":
        print("requeued" if q.requeue(args.job_id) else "not found or not dead/cancelled")

    elif args.cmd == "cancel":
        print("cancelled" if q.cancel(args.job_id) else "not found or not queued")


if __name__ == "__main__":
    main()
