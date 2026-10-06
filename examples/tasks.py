"""Example task handlers. Run a worker with:
    python -m jobqueue worker --tasks examples.tasks
"""
import random
import time

from jobqueue import TaskRegistry

tasks = TaskRegistry()


@tasks.task("send_email")
def send_email(payload):
    time.sleep(0.05)  # pretend to call an SMTP server
    return {"delivered_to": payload["to"]}


@tasks.task("resize_image")
def resize_image(payload):
    time.sleep(0.1)
    w, h = payload["width"], payload["height"]
    return {"thumbnail": [w // 4, h // 4]}


@tasks.task("call_flaky_api")
def call_flaky_api(payload):
    """Fails 50% of the time, like a real third-party API having a bad day.
    Retries with backoff usually get it through."""
    if random.random() < 0.5:
        raise ConnectionError("upstream returned 503")
    return "ok"


@tasks.task("charge_card")
def charge_card(payload):
    """Always fails for invalid cards, so it ends up in the dead-letter queue."""
    if payload.get("card") == "invalid":
        raise ValueError("card declined")
    return {"charged": payload["amount"]}
