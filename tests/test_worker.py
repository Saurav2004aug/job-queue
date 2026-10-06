"""Worker pool and concurrency tests (real threads, real processes, real time)."""
import multiprocessing
import os
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from jobqueue import DEAD, SUCCEEDED, JobQueue, TaskRegistry, Worker  # noqa: E402


class WorkerCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "q.db")
        self.q = JobQueue(self.path, backoff_base=0.01, jitter=0)

    def tearDown(self):
        self.q.close()
        self.tmp.cleanup()


class TestWorker(WorkerCase):
    def test_processes_all_jobs_and_stores_results(self):
        reg = TaskRegistry()

        @reg.task("square")
        def square(p):
            return p["n"] ** 2

        ids = [self.q.enqueue("square", {"n": i}) for i in range(50)]
        w = Worker(self.q, reg, concurrency=4, poll_interval=0.01)
        w.run_until_empty(timeout=20)
        self.assertEqual(w.stats["succeeded"], 50)
        self.assertEqual([self.q.get(i).result for i in ids], [i * i for i in range(50)])

    def test_retries_then_succeeds(self):
        reg = TaskRegistry()
        calls = []

        @reg.task()
        def flaky(p):
            calls.append(1)
            if len(calls) < 3:
                raise ConnectionError("try again")
            return "finally"

        jid = self.q.enqueue("flaky", max_attempts=5)
        w = Worker(self.q, reg, concurrency=1, poll_interval=0.01)
        w.run_until_empty(timeout=10)
        job = self.q.get(jid)
        self.assertEqual((job.status, job.attempts, job.result), (SUCCEEDED, 3, "finally"))
        self.assertIn("ConnectionError: try again", job.last_error)

    def test_unknown_task_goes_to_dead_letter(self):
        jid = self.q.enqueue("does_not_exist", max_attempts=2)
        w = Worker(self.q, TaskRegistry(), concurrency=1, poll_interval=0.01)
        w.run_until_empty(timeout=10)
        job = self.q.get(jid)
        self.assertEqual(job.status, DEAD)
        self.assertIn("no handler registered", job.last_error)

    def test_graceful_stop_finishes_in_flight_job(self):
        reg = TaskRegistry()
        started = threading.Event()

        @reg.task()
        def slow(p):
            started.set()
            time.sleep(0.5)
            return "done"

        jid = self.q.enqueue("slow")
        self.q.enqueue("slow")  # should NOT be started after stop()
        w = Worker(self.q, reg, concurrency=1, poll_interval=0.01).start()
        started.wait(5)
        w.stop()
        self.assertEqual(self.q.get(jid).status, SUCCEEDED)
        self.assertEqual(self.q.stats()["queued"], 1)

    def test_heartbeat_keeps_long_job_from_being_stolen(self):
        q = JobQueue(self.path, lease_seconds=0.3)
        reg = TaskRegistry()
        runs = []

        @reg.task()
        def long_job(p):
            runs.append(threading.current_thread().name)
            time.sleep(1.2)  # 4x the lease; only heartbeats keep ownership
            return "ok"

        q.enqueue("long_job")
        w = Worker(q, reg, concurrency=3, poll_interval=0.02)
        w.run_until_empty(timeout=10)
        self.assertEqual(len(runs), 1, "job was executed more than once")
        q.close()


class TestExactlyOnceClaim(WorkerCase):
    N_JOBS = 400

    def test_many_threads_never_double_claim(self):
        for i in range(self.N_JOBS):
            self.q.enqueue("t", {"i": i})
        claimed, lock = [], threading.Lock()

        def grab(name):
            q = JobQueue(self.path)
            while (job := q.claim(name)) is not None:
                q.complete(job.id, name)
                with lock:
                    claimed.append(job.id)
            q.close()

        threads = [threading.Thread(target=grab, args=(f"t{i}",)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(claimed), self.N_JOBS)
        self.assertEqual(len(set(claimed)), self.N_JOBS)  # no job claimed twice

    def test_many_processes_never_double_claim(self):
        for i in range(self.N_JOBS):
            self.q.enqueue("t", {"i": i})
        ctx = multiprocessing.get_context("spawn")
        results = ctx.Queue()
        procs = [ctx.Process(target=_claim_all, args=(self.path, f"p{i}", results)) for i in range(4)]
        for p in procs:
            p.start()
        claimed = []
        for _ in procs:
            claimed += results.get(timeout=60)
        for p in procs:
            p.join(10)
        self.assertEqual(len(claimed), self.N_JOBS)
        self.assertEqual(len(set(claimed)), self.N_JOBS)
        self.assertEqual(self.q.stats()["succeeded"], self.N_JOBS)


def _claim_all(path, name, results):
    q = JobQueue(path)
    got = []
    while (job := q.claim(name)) is not None:
        q.complete(job.id, name)
        got.append(job.id)
    results.put(got)


if __name__ == "__main__":
    unittest.main()
