"""
A durable job queue backed by SQLite, with no broker to run.

Guarantees and mechanics:
  * Durability: jobs live in a SQLite database (WAL mode), so they survive
    crashes and restarts.
  * No double claims: a job is claimed inside a `BEGIN IMMEDIATE`
    transaction, which takes SQLite's write lock. Two workers, whether threads
    or separate processes, can never hold a live lease on the same job.
  * At-least-once execution: a claimed job holds a *lease* (visibility
    timeout). If the worker dies without finishing, the lease expires and
    another worker picks the job up again, as SQS does. Handlers should
    therefore be idempotent.
  * Retries with exponential backoff and jitter: delay = base * 2^(attempt-1),
    capped, with random jitter so failing jobs don't retry in lockstep.
  * Dead-letter queue: a job that fails `max_attempts` times is marked `dead`
    for inspection and manual requeue instead of retrying forever.
  * Priorities, delayed/scheduled jobs (`run_at`), and idempotency keys for
    safe re-enqueueing.
"""

import json
import os
import random
import socket
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

QUEUED, RUNNING, SUCCEEDED, DEAD, CANCELLED = "queued", "running", "succeeded", "dead", "cancelled"

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    queue           TEXT    NOT NULL DEFAULT 'default',
    task            TEXT    NOT NULL,
    payload         TEXT    NOT NULL,
    status          TEXT    NOT NULL DEFAULT 'queued',
    priority        INTEGER NOT NULL DEFAULT 0,
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    run_at          REAL    NOT NULL,
    locked_by       TEXT,
    lease_expires   REAL,
    idempotency_key TEXT    UNIQUE,
    last_error      TEXT,
    result          TEXT,
    created_at      REAL    NOT NULL,
    finished_at     REAL
);
-- The claim query filters on (queue, status, run_at) and orders by priority:
CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs(queue, status, priority DESC, run_at, id);
CREATE INDEX IF NOT EXISTS idx_jobs_lease ON jobs(status, lease_expires);
"""


@dataclass
class Job:
    id: int
    queue: str
    task: str
    payload: Any
    status: str
    priority: int
    attempts: int
    max_attempts: int
    run_at: float
    locked_by: Optional[str]
    lease_expires: Optional[float]
    idempotency_key: Optional[str]
    last_error: Optional[str]
    result: Any
    created_at: float
    finished_at: Optional[float]

    @classmethod
    def from_row(cls, row: sqlite3.Row) -> "Job":
        d = dict(row)
        d["payload"] = json.loads(d["payload"])
        d["result"] = json.loads(d["result"]) if d["result"] is not None else None
        return cls(**d)


class JobQueue:
    def __init__(self, path: str, *, lease_seconds: float = 30.0,
                 backoff_base: float = 1.0, backoff_max: float = 300.0, jitter: float = 0.1,
                 clock: Callable[[], float] = time.time, rng: Optional[random.Random] = None):
        self.path = path
        self.lease_seconds = lease_seconds
        self.backoff_base = backoff_base
        self.backoff_max = backoff_max
        self.jitter = jitter
        self.clock = clock
        self.rng = rng or random.Random()
        self._local = threading.local()  # one connection per thread
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(SCHEMA)

    # ------------------------------------------------------------------
    # Connection handling
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        # isolation_level=None: we issue BEGIN/COMMIT ourselves for precise locking.
        db = sqlite3.connect(self.path, timeout=30, isolation_level=None, check_same_thread=False)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA busy_timeout=30000")
        db.execute("PRAGMA synchronous=NORMAL")  # safe with WAL, much faster than FULL
        return db

    @property
    def db(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._local.conn = self._connect()
        return conn

    def _write_txn(self, fn):
        """Run fn(db) inside BEGIN IMMEDIATE, so we hold the write lock for the
        whole read-then-update. This is what makes claiming race-free."""
        db = self.db
        db.execute("BEGIN IMMEDIATE")
        try:
            result = fn(db)
            db.execute("COMMIT")
            return result
        except BaseException:
            db.execute("ROLLBACK")
            raise

    def close(self):
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    # ------------------------------------------------------------------
    # Producer API
    # ------------------------------------------------------------------

    def enqueue(self, task: str, payload: Any = None, *, queue: str = "default",
                priority: int = 0, delay: float = 0.0, run_at: Optional[float] = None,
                max_attempts: int = 3, idempotency_key: Optional[str] = None) -> int:
        """Add a job. Returns its id. With an idempotency_key, enqueueing the
        same key twice returns the original job id instead of a duplicate."""
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        now = self.clock()
        body = json.dumps(payload)

        def txn(db):
            if idempotency_key is not None:
                row = db.execute("SELECT id FROM jobs WHERE idempotency_key = ?",
                                 (idempotency_key,)).fetchone()
                if row:
                    return row["id"]
            cur = db.execute(
                """INSERT INTO jobs (queue, task, payload, priority, max_attempts, run_at,
                                     idempotency_key, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (queue, task, body, priority, max_attempts,
                 run_at if run_at is not None else now + delay, idempotency_key, now))
            return cur.lastrowid

        return self._write_txn(txn)

    # ------------------------------------------------------------------
    # Consumer API
    # ------------------------------------------------------------------

    def claim(self, worker_id: str, queues: tuple[str, ...] = ("default",)) -> Optional[Job]:
        """Atomically take the highest-priority runnable job, or return None.

        Runnable means queued and due, OR running with an expired lease (the
        worker that held it is presumed dead)."""
        now = self.clock()
        placeholders = ",".join("?" * len(queues))

        def txn(db):
            # Two separate indexed lookups rather than one query with an OR:
            # SQLite can't use an index for "(A AND B) OR (C AND D)", so the
            # combined form scanned every job in the queue on every claim.
            # 1) a job whose worker died (lease expired): it has waited longest.
            #    A job that has already used every attempt goes to the
            #    dead-letter state instead. Otherwise a job that crashes its
            #    worker every time (e.g. out of memory) would retry forever.
            while True:
                row = db.execute(
                    f"""SELECT id, attempts, max_attempts FROM jobs
                        WHERE status = 'running' AND lease_expires < ? AND queue IN ({placeholders})
                        ORDER BY lease_expires LIMIT 1""",
                    (now, *queues)).fetchone()
                if row is None or row["attempts"] < row["max_attempts"]:
                    break
                db.execute(
                    """UPDATE jobs SET status = 'dead', finished_at = ?, locked_by = NULL,
                                       lease_expires = NULL, last_error = ?
                       WHERE id = ?""",
                    (now, f"lease expired on final attempt {row['attempts']}/{row['max_attempts']} "
                          "(worker crashed or hung)", row["id"]))
            # 2) otherwise the highest-priority due job (walks idx_jobs_claim in order)
            if row is None:
                row = db.execute(
                    f"""SELECT id FROM jobs
                        WHERE queue IN ({placeholders}) AND status = 'queued' AND run_at <= ?
                        ORDER BY priority DESC, run_at, id
                        LIMIT 1""",
                    (*queues, now)).fetchone()
            if row is None:
                return None
            db.execute(
                """UPDATE jobs SET status = 'running', locked_by = ?, lease_expires = ?,
                                   attempts = attempts + 1
                   WHERE id = ?""",
                (worker_id, now + self.lease_seconds, row["id"]))
            return Job.from_row(db.execute("SELECT * FROM jobs WHERE id = ?", (row["id"],)).fetchone())

        return self._write_txn(txn)

    def heartbeat(self, job_id: int, worker_id: str) -> bool:
        """Extend the lease of a long-running job. Returns False if this worker
        no longer owns the job (its lease expired and someone else took it)."""
        cur = self.db.execute(
            "UPDATE jobs SET lease_expires = ? WHERE id = ? AND status = 'running' AND locked_by = ?",
            (self.clock() + self.lease_seconds, job_id, worker_id))
        return cur.rowcount == 1

    def complete(self, job_id: int, worker_id: str, result: Any = None) -> bool:
        """Mark success. The locked_by check stops a 'zombie' worker (whose lease
        expired) from overwriting the outcome of the worker that took over."""
        cur = self.db.execute(
            """UPDATE jobs SET status = 'succeeded', result = ?, finished_at = ?,
                               locked_by = NULL, lease_expires = NULL
               WHERE id = ? AND status = 'running' AND locked_by = ?""",
            (json.dumps(result), self.clock(), job_id, worker_id))
        return cur.rowcount == 1

    def fail(self, job_id: int, worker_id: str, error: str) -> Optional[str]:
        """Record a failure. Schedules a retry with backoff, or moves the job to
        the dead-letter state. Returns the new status (None if not owned)."""
        now = self.clock()

        def txn(db):
            row = db.execute("SELECT attempts, max_attempts FROM jobs WHERE id = ? "
                             "AND status = 'running' AND locked_by = ?",
                             (job_id, worker_id)).fetchone()
            if row is None:
                return None
            if row["attempts"] >= row["max_attempts"]:
                db.execute("""UPDATE jobs SET status = 'dead', last_error = ?, finished_at = ?,
                                              locked_by = NULL, lease_expires = NULL WHERE id = ?""",
                           (error, now, job_id))
                return DEAD
            db.execute("""UPDATE jobs SET status = 'queued', last_error = ?, run_at = ?,
                                          locked_by = NULL, lease_expires = NULL WHERE id = ?""",
                       (error, now + self.backoff_delay(row["attempts"]), job_id))
            return QUEUED

        return self._write_txn(txn)

    def backoff_delay(self, attempt: int) -> float:
        """Exponential backoff with jitter: base * 2^(attempt-1), capped, ±jitter."""
        delay = min(self.backoff_max, self.backoff_base * (2 ** (attempt - 1)))
        return delay * (1 + self.rng.uniform(-self.jitter, self.jitter))

    # ------------------------------------------------------------------
    # Admin API
    # ------------------------------------------------------------------

    def get(self, job_id: int) -> Optional[Job]:
        row = self.db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        return Job.from_row(row) if row else None

    def list(self, status: Optional[str] = None, limit: int = 50) -> list[Job]:
        if status:
            rows = self.db.execute("SELECT * FROM jobs WHERE status = ? ORDER BY id DESC LIMIT ?",
                                   (status, limit))
        else:
            rows = self.db.execute("SELECT * FROM jobs ORDER BY id DESC LIMIT ?", (limit,))
        return [Job.from_row(r) for r in rows]

    def requeue(self, job_id: int) -> bool:
        """Give a dead (or cancelled) job a fresh set of attempts."""
        cur = self.db.execute(
            """UPDATE jobs SET status = 'queued', attempts = 0, run_at = ?, finished_at = NULL
               WHERE id = ? AND status IN ('dead', 'cancelled')""", (self.clock(), job_id))
        return cur.rowcount == 1

    def cancel(self, job_id: int) -> bool:
        cur = self.db.execute("UPDATE jobs SET status = 'cancelled', finished_at = ? "
                              "WHERE id = ? AND status = 'queued'", (self.clock(), job_id))
        return cur.rowcount == 1

    def stats(self) -> dict:
        counts = {s: 0 for s in (QUEUED, RUNNING, SUCCEEDED, DEAD, CANCELLED)}
        for row in self.db.execute("SELECT status, COUNT(*) AS n FROM jobs GROUP BY status"):
            counts[row["status"]] = row["n"]
        now = self.clock()
        counts["due_now"] = self.db.execute(
            "SELECT COUNT(*) FROM jobs WHERE status = 'queued' AND run_at <= ?", (now,)).fetchone()[0]
        counts["stuck"] = self.db.execute(  # running jobs whose worker's lease expired
            "SELECT COUNT(*) FROM jobs WHERE status = 'running' AND lease_expires < ?", (now,)).fetchone()[0]
        return counts

    def purge(self, older_than_seconds: float) -> int:
        """Delete finished jobs older than the cutoff (retention)."""
        cur = self.db.execute("DELETE FROM jobs WHERE status IN ('succeeded', 'cancelled') "
                              "AND finished_at < ?", (self.clock() - older_than_seconds,))
        return cur.rowcount


def default_worker_id() -> str:
    """host:pid identifies a worker process; threads append #n."""
    return f"{socket.gethostname()}:{os.getpid()}"
