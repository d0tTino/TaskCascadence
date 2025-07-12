"""Utilities for emitting TaskRun and TaskSpec to UME."""

from __future__ import annotations

import threading
import time
from typing import Any

from .models import TaskRun, TaskSpec


def _queue_within_deadline(obj: Any, client: Any, max_delay: float = 0.2) -> threading.Thread:
    """Queue *obj* to *client* in a background thread within ``max_delay`` seconds."""

    def _send() -> None:
        client.enqueue(obj)

    thread = threading.Thread(target=_send, daemon=True)
    start = time.monotonic()
    thread.start()
    thread.join(timeout=max_delay)
    elapsed = time.monotonic() - start
    if elapsed > max_delay:
        raise RuntimeError(
            f"Emission to client exceeded {max_delay}s deadline (took {elapsed:.3f}s)"
        )
    return thread


def emit_task_spec(spec: TaskSpec, client: Any) -> None:
    """Emit ``TaskSpec`` information to ``client``."""

    _queue_within_deadline(spec, client)


def emit_task_run(run: TaskRun, client: Any) -> None:
    """Emit ``TaskRun`` information to ``client``."""

    _queue_within_deadline(run, client)
