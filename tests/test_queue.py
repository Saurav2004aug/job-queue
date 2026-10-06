"""Queue semantics, tested single-threaded with a fake clock (no sleeps)."""
import os
import random
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from jobqueue import CANCELLED, DEAD, QUEUED, RUNNING, SUCCEEDED, JobQueue  # noqa: E402


class FakeClock:
    def __init__(self, t=1_000_000.0):
        self.t = t

    def __call__(self):
        return self.t


class QueueCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.clock = FakeClock()
        self.q = JobQueue(os.path.join(self.tmp.name, "q.db"), clock=self.clock,
                          lease_seconds=30, backoff_base=2, backoff_max=60, jitter=0)

    def tearDown(self):
        self.q.close()
        self.tmp.cleanup()


class TestEnqueueAndClaim(QueueCase):
    def test_payload_roundtrip_and_claim_marks_running(self):
        jid = self.q.enqueue("email", {"to": "a@b.com", "tags": [1, 2]})
        job = self.q.claim("w1")
        self.assertEqual((job.id, job.task, job.payload), (jid, "email", {"to": "a@b.com", "tags": [1, 2]}))
        self.assertEqual((job.status, job.attempts, job.locked_by), (RUNNING, 1, "w1"))
        self.assertEqual(job.lease_expires, self.clock.t + 30)
        self.assertIsNone(self.q.claim("w2"))  # nothing else to claim

    def test_priority_then_fifo(self):
        low1 = self.q.enqueue("t", priority=0)
        high = self.q.enqueue("t", priority=10)
        low2 = self.q.enqueue("t", priority=0)
        mid = self.q.enqueue("t", priority=5)
        order = [self.q.claim("w").id for _ in range(4)]
        self.assertEqual(order, [high, mid, low1, low2])

    def test_delayed_job_not_claimable_until_due(self):
        self.q.enqueue("t", delay=60)
        self.assertIsNone(self.q.claim("w"))
        self.clock.t += 59.9
        self.assertIsNone(self.q.claim("w"))
        self.clock.t += 0.1
        self.assertIsNotNone(self.q.claim("w"))

    def test_scheduled_run_at(self):
        self.q.enqueue("t", run_at=self.clock.t + 3600)
        self.assertEqual(self.q.stats()["due_now"], 0)
        self.clock.t += 3600
        self.assertEqual(self.q.stats()["due_now"], 1)

    def test_named_queues_are_isolated(self):
        self.q.enqueue("t", queue="emails")
        self.q.enqueue("t", queue="reports")
        self.assertIsNone(self.q.claim("w", queues=("default",)))
        self.assertEqual(self.q.claim("w", queues=("reports",)).queue, "reports")
        self.assertEqual(self.q.claim("w", queues=("emails", "reports")).queue, "emails")

    def test_idempotency_key_prevents_duplicates(self):
        a = self.q.enqueue("charge", {"order": 1}, idempotency_key="order-1")
        b = self.q.enqueue("charge", {"order": 1}, idempotency_key="order-1")
        self.assertEqual(a, b)
        self.assertEqual(self.q.stats()["queued"], 1)

    def test_validation(self):
        with self.assertRaises(ValueError):
            self.q.enqueue("t", max_attempts=0)


class TestCompletionAndRetries(QueueCase):
    def test_complete_stores_result(self):
        jid = self.q.enqueue("t")
        self.q.claim("w")
        self.assertTrue(self.q.complete(jid, "w", {"ok": True}))
        job = self.q.get(jid)
        self.assertEqual((job.status, job.result, job.finished_at), (SUCCEEDED, {"ok": True}, self.clock.t))

    def test_exponential_backoff_schedule(self):
        jid = self.q.enqueue("t", max_attempts=5)
        expected_delays = [2, 4, 8, 16]  # base * 2^(attempt-1)
        for delay in expected_delays:
            self.q.claim("w")
            self.assertEqual(self.q.fail(jid, "w", "boom"), QUEUED)
            self.assertAlmostEqual(self.q.get(jid).run_at - self.clock.t, delay)
            self.assertIsNone(self.q.claim("w"))  # not due yet
            self.clock.t += delay
        self.q.claim("w")
        self.assertEqual(self.q.fail(jid, "w", "final boom"), DEAD)
        job = self.q.get(jid)
        self.assertEqual((job.status, job.attempts, job.last_error), (DEAD, 5, "final boom"))

    def test_backoff_is_capped_and_jittered(self):
        q = JobQueue(os.path.join(self.tmp.name, "j.db"), backoff_base=1, backoff_max=10,
                     jitter=0.2, rng=random.Random(1))
        self.assertEqual(max(q.backoff_delay(50) for _ in range(100)) <= 12, True)
        delays = {round(q.backoff_delay(3), 3) for _ in range(20)}
        self.assertGreater(len(delays), 1)                   # jitter spreads retries out
        self.assertTrue(all(3.2 <= d <= 4.8 for d in delays))
        q.close()

    def test_requeue_dead_job_resets_attempts(self):
        jid = self.q.enqueue("t", max_attempts=1)
        self.q.claim("w")
        self.q.fail(jid, "w", "boom")
        self.assertTrue(self.q.requeue(jid))
        job = self.q.claim("w")
        self.assertEqual((job.id, job.attempts), (jid, 1))
        self.assertFalse(self.q.requeue(jid))  # running jobs can't be requeued

    def test_cancel_only_queued(self):
        jid = self.q.enqueue("t")
        self.assertTrue(self.q.cancel(jid))
        self.assertEqual(self.q.get(jid).status, CANCELLED)
        self.assertIsNone(self.q.claim("w"))


class TestLeases(QueueCase):
    def test_crashed_worker_job_is_reclaimed_after_lease(self):
        jid = self.q.enqueue("t")
        self.q.claim("crashed-worker")         # ...and then the worker dies
        self.clock.t += 29
        self.assertIsNone(self.q.claim("w2"))
        self.assertEqual(self.q.stats()["stuck"], 0)
        self.clock.t += 2
        self.assertEqual(self.q.stats()["stuck"], 1)
        job = self.q.claim("w2")
        self.assertEqual((job.id, job.locked_by, job.attempts), (jid, "w2", 2))

    def test_job_that_keeps_crashing_workers_is_dead_lettered(self):
        jid = self.q.enqueue("oom_job", max_attempts=2)
        self.q.claim("w1")                     # attempt 1: worker crashes
        self.clock.t += 31
        self.assertEqual(self.q.claim("w2").attempts, 2)   # attempt 2: crashes too
        self.clock.t += 31
        self.assertIsNone(self.q.claim("w3"))  # no third attempt
        job = self.q.get(jid)
        self.assertEqual(job.status, DEAD)
        self.assertIn("lease expired on final attempt 2/2", job.last_error)

    def test_zombie_worker_cannot_overwrite_new_owner(self):
        jid = self.q.enqueue("t")
        self.q.claim("slow")
        self.clock.t += 31
        self.q.claim("fast")
        self.assertFalse(self.q.complete(jid, "slow", "stale result"))
        self.assertIsNone(self.q.fail(jid, "slow", "stale error"))
        self.assertFalse(self.q.heartbeat(jid, "slow"))
        self.assertTrue(self.q.complete(jid, "fast", "fresh result"))
        self.assertEqual(self.q.get(jid).result, "fresh result")

    def test_heartbeat_extends_lease(self):
        jid = self.q.enqueue("t")
        self.q.claim("w")
        for _ in range(5):
            self.clock.t += 20
            self.assertTrue(self.q.heartbeat(jid, "w"))
        self.assertIsNone(self.q.claim("other"))  # 100s later, still owned by w


class TestAdmin(QueueCase):
    def test_stats_and_list(self):
        a = self.q.enqueue("t")
        self.q.enqueue("t")
        self.q.enqueue("t", delay=100)
        self.q.claim("w")
        self.q.complete(a, "w")
        s = self.q.stats()
        self.assertEqual((s["succeeded"], s["queued"], s["due_now"]), (1, 2, 1))
        self.assertEqual([j.status for j in self.q.list(status=SUCCEEDED)], [SUCCEEDED])

    def test_purge_old_finished_jobs(self):
        a = self.q.enqueue("t")
        self.q.claim("w")
        self.q.complete(a, "w")
        self.q.enqueue("t")
        self.clock.t += 86400 * 8
        self.assertEqual(self.q.purge(older_than_seconds=86400 * 7), 1)
        self.assertEqual(self.q.stats()["queued"], 1)  # unfinished jobs are kept

    def test_survives_reopen(self):
        jid = self.q.enqueue("t", {"x": 1})
        self.q.close()
        q2 = JobQueue(self.q.path, clock=self.clock)
        self.assertEqual(q2.claim("w").id, jid)
        q2.close()


if __name__ == "__main__":
    unittest.main()
