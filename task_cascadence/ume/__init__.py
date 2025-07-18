"""Utilities for emitting TaskRun and TaskSpec to UME."""

from __future__ import annotations

import threading
import time
from typing import Any
import hashlib

from ..transport import BaseTransport, get_client
from .models import TaskRun, TaskSpec


_default_client: BaseTransport | None = None


def _hash_user_id(user_id: str) -> str:
    return hashlib.sha256(user_id.encode()).hexdigest()


def configure_transport(transport: str, **kwargs: Any) -> None:
    """Configure the default transport client."""

    global _default_client
    _default_client = get_client(transport, **kwargs)


def _queue_within_deadline(obj: Any, client: Any, max_delay: float = 0.2) -> threading.Thread:
    """Queue *obj* to *client* in a background thread within ``max_delay`` seconds.

    If the deadline is exceeded the thread is left running as a daemon and a
    :class:`RuntimeError` is raised. In that situation the event may be dropped
    before reaching the client.
    """

    def _send() -> None:
        client.enqueue(obj)

    thread = threading.Thread(target=_send, daemon=True)
    start = time.monotonic()
    thread.start()
    thread.join(timeout=max_delay)
    elapsed = time.monotonic() - start
    if thread.is_alive():
        # the background thread continues to run but is detached via ``daemon``
        # so that it won't block interpreter shutdown
        raise RuntimeError(
            f"Emission to client exceeded {max_delay}s deadline (thread still running)"
        )
    if elapsed > max_delay:
        raise RuntimeError(
            f"Emission to client exceeded {max_delay}s deadline (took {elapsed:.3f}s)"
        )
    return thread


def emit_task_spec(
    spec: TaskSpec, client: Any | None = None, user_id: str | None = None
) -> None:
    """Emit ``TaskSpec`` information to ``client`` or the configured default."""

    target = client or _default_client
    if target is None:
        raise ValueError("No transport client configured")
    if user_id is not None:
        spec.user_hash = _hash_user_id(user_id)
    _queue_within_deadline(spec, target)


def emit_task_run(
    run: TaskRun, client: Any | None = None, user_id: str | None = None
) -> None:
    """Emit ``TaskRun`` information to ``client`` or the configured default."""

    target = client or _default_client
    if target is None:
        raise ValueError("No transport client configured")
    if user_id is not None:
        run.user_hash = _hash_user_id(user_id)
    _queue_within_deadline(run, target)
