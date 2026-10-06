# Job Queue — Durable Concurrent Background Job System

[![CI](https://github.com/YOUR_GITHUB_USERNAME/job-queue/actions/workflows/ci.yml/badge.svg)](https://github.com/YOUR_GITHUB_USERNAME/job-queue/actions)

A persistent background job queue implemented in Python and SQL, designed around **lease-based delivery, retries, concurrency safety and failure recovery**.

The project demonstrates backend and distributed-systems patterns without depending on a managed queue service.

## Highlights

- Persistent queue backed by SQLite WAL
- Lease-based job claiming
- At-least-once delivery semantics
- Worker heartbeats
- Exponential-backoff retries with jitter
- Dead-letter queue
- Multi-thread and multi-process concurrency
- Graceful worker shutdown
- Idempotency-oriented job processing
- Query-plan-driven performance optimization
- Benchmark tooling

## Architecture

```text
Producer
   |
   v
SQLite WAL
   |
   v
Claim + Lease
   |
   +------> Worker 1
   |
   +------> Worker 2
   |
   +------> Worker N
              |
        +-----+-----+
        |           |
      Success     Failure
                    |
                 Retry
                    |
                 Backoff
                    |
              Dead Letter
```

## Tech Stack

- Python
- SQLite / WAL
- SQL
- Threads and processes
- unittest
- Benchmarking

## Failure model

A worker does not permanently own a job. It receives a lease and periodically renews it.

If the worker dies or stops renewing the lease, another worker can reclaim the job after the lease expires.

This provides **at-least-once delivery**, meaning a job can execute more than once and job handlers should therefore be designed to be idempotent where necessary.

## Retries

Failed jobs are retried using exponential backoff and jitter to avoid synchronized retry storms.

Jobs that exceed the retry limit are moved to the dead-letter queue.

## Concurrency

The test suite intentionally races multiple processes and threads against the same queue to validate claiming behavior.

## Performance optimization

The project includes a measured database optimization. Query-plan analysis identified an avoidable full-table scan; indexing/query changes increased measured throughput from approximately **226 jobs/sec to 2,600 jobs/sec** in the benchmark scenario.

This is an optimization result for the included benchmark—not a universal throughput guarantee.

## Run locally

```bash
pip install -r requirements.txt
python -m unittest discover -s tests -v
```

Demo:

```bash
python examples/demo.py
```

Benchmark:

```bash
python benchmark.py
```

## Interview topics

- At-least-once vs exactly-once delivery
- Why leases are needed
- Zombie workers
- Heartbeats
- Retry storms and jitter
- Idempotency
- SQLite WAL
- Race conditions in job claiming
- Database indexes and query plans
- Graceful shutdown
