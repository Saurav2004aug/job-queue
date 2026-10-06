"""jobqueue: a durable background job queue with retries, built on SQLite."""

from .queue import CANCELLED, DEAD, QUEUED, RUNNING, SUCCEEDED, Job, JobQueue
from .worker import TaskRegistry, Worker

__all__ = ["JobQueue", "Job", "Worker", "TaskRegistry",
           "QUEUED", "RUNNING", "SUCCEEDED", "DEAD", "CANCELLED"]
__version__ = "1.0.0"
