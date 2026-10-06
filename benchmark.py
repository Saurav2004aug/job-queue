"""
Throughput benchmark: enqueue N no-op jobs, then drain them with a worker pool.
Run: python benchmark.py
"""
import os
import tempfile
import time

from jobqueue import JobQueue, TaskRegistry, Worker

N = 5000
reg = TaskRegistry()


@reg.task()
def noop(payload):
    return None


if __name__ == "__main__":
    q = JobQueue(os.path.join(tempfile.mkdtemp(), "bench.db"))
    t0 = time.perf_counter()
    for i in range(N):
        q.enqueue("noop", {"i": i})
    enqueue_s = time.perf_counter() - t0
    print(f"enqueue: {N / enqueue_s:,.0f} jobs/sec")

    for threads in (1, 4):
        if threads != 1:  # the first round drains the jobs enqueued above
            for i in range(N):
                q.enqueue("noop", {"i": i})
        t0 = time.perf_counter()
        Worker(q, reg, concurrency=threads, poll_interval=0.01).run_until_empty(timeout=300)
        print(f"process ({threads} thread{'s' if threads > 1 else ''}): "
              f"{N / (time.perf_counter() - t0):,.0f} jobs/sec (claim + complete, fully durable)")
