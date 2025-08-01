"""Utilities for emitting TaskRun and TaskSpec to UME."""

from __future__ import annotations

import threading
import time
import asyncio
from typing import Any
import hashlib
from ..config import load_config

from ..transport import BaseTransport, AsyncBaseTransport, get_client
from .models import (
    TaskRun,
    TaskSpec,
    PointerUpdate,
    TaskNote,
    IdeaSeed,
    StageUpdate,
)
from ..stage_store import StageStore
from ..idea_store import IdeaStore


_default_client: BaseTransport | AsyncBaseTransport | None = None
_stage_store: StageStore | None = None
_idea_store: IdeaStore | None = None


def _hash_user_id(user_id: str) -> str:
    secret = load_config().get("hash_secret", "")
    return hashlib.sha256((secret + user_id).encode()).hexdigest()


def _get_stage_store() -> StageStore:
    global _stage_store
    if _stage_store is None:
        _stage_store = StageStore()
    return _stage_store


def _get_idea_store() -> IdeaStore:
    global _idea_store
    if _idea_store is None:
        _idea_store = IdeaStore()
    return _idea_store


def emit_stage_update(task_name: str, stage: str, user_id: str | None = None) -> None:
    """Persist a pipeline stage event via :class:`StageStore`."""

    store = _get_stage_store()
    user_hash = _hash_user_id(user_id) if user_id is not None else None
    store.add_event(task_name, stage, user_hash)


def emit_stage_update_event(
    task_name: str,
    stage: str,
    client: Any | None = None,
    user_id: str | None = None,
    *,
    use_asyncio: bool = False,
) -> asyncio.Task | threading.Thread | None:
    """Persist and emit ``StageUpdate`` using the configured transport."""

    emit_stage_update(task_name, stage, user_id=user_id)
    target = client or _default_client
    if target is None:
        return None
    update = StageUpdate(task_name=task_name, stage=stage)
    if user_id is not None:
        update.user_hash = _hash_user_id(user_id)
    if use_asyncio:
        return asyncio.get_running_loop().create_task(
            _async_queue_within_deadline(update, target)
        )
    return _queue_within_deadline(update, target)


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


async def _async_queue_within_deadline(
    obj: Any, client: Any, max_delay: float = 0.2
) -> asyncio.Task:
    """Asynchronously queue *obj* to *client* within ``max_delay`` seconds."""

    async def _send() -> None:
        await client.enqueue(obj)

    loop = asyncio.get_running_loop()
    task = loop.create_task(_send())
    start = loop.time()
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=max_delay)
    except asyncio.TimeoutError:
        raise RuntimeError(
            f"Emission to client exceeded {max_delay}s deadline (task still running)"
        ) from None
    elapsed = loop.time() - start
    if elapsed > max_delay:
        raise RuntimeError(
            f"Emission to client exceeded {max_delay}s deadline (took {elapsed:.3f}s)"
        )
    return task


def emit_task_spec(
    spec: TaskSpec,
    client: Any | None = None,
    user_id: str | None = None,
    *,
    use_asyncio: bool = False,
) -> asyncio.Task | threading.Thread | None:
    """Emit ``TaskSpec`` information to ``client`` or the configured default."""

    target = client or _default_client
    if target is None:
        raise ValueError("No transport client configured")
    if user_id is not None:
        spec.user_hash = _hash_user_id(user_id)
    if use_asyncio:
        return asyncio.get_running_loop().create_task(
            _async_queue_within_deadline(spec, target)
        )
    return _queue_within_deadline(spec, target)


def emit_task_run(
    run: TaskRun,
    client: Any | None = None,
    user_id: str | None = None,
    *,
    use_asyncio: bool = False,
) -> asyncio.Task | threading.Thread | None:
    """Emit ``TaskRun`` information to ``client`` or the configured default."""

    target = client or _default_client
    if target is None:
        raise ValueError("No transport client configured")
    if user_id is not None:
        run.user_hash = _hash_user_id(user_id)
    if use_asyncio:
        return asyncio.get_running_loop().create_task(
            _async_queue_within_deadline(run, target)
        )
    return _queue_within_deadline(run, target)


def emit_pointer_update(
    update: PointerUpdate,
    client: Any | None = None,
    *,
    use_asyncio: bool = False,
) -> asyncio.Task | threading.Thread | None:
    """Emit ``PointerUpdate`` using the configured transport."""

    target = client or _default_client
    if target is None:
        raise ValueError("No transport client configured")
    if use_asyncio:
        return asyncio.get_running_loop().create_task(
            _async_queue_within_deadline(update, target)
        )
    return _queue_within_deadline(update, target)


def emit_task_note(
    note: TaskNote,
    client: Any | None = None,
    user_id: str | None = None,
    *,
    use_asyncio: bool = False,
) -> asyncio.Task | threading.Thread | None:
    """Emit ``TaskNote`` using the configured transport."""

    target = client or _default_client
    if target is None:
        raise ValueError("No transport client configured")
    if user_id is not None:
        note.user_hash = _hash_user_id(user_id)
    if use_asyncio:
        return asyncio.get_running_loop().create_task(
            _async_queue_within_deadline(note, target)
        )
    return _queue_within_deadline(note, target)


def emit_idea_seed(
    seed: IdeaSeed,
    client: Any | None = None,
    user_id: str | None = None,
    *,
    use_asyncio: bool = False,
) -> asyncio.Task | threading.Thread | None:
    """Emit ``IdeaSeed`` using the configured transport."""

    target = client or _default_client
    if target is None:
        raise ValueError("No transport client configured")
    if user_id is not None:
        seed.user_hash = _hash_user_id(user_id)
    _get_idea_store().add_seed(seed)
    if use_asyncio:
        return asyncio.get_running_loop().create_task(
            _async_queue_within_deadline(seed, target)
        )
    return _queue_within_deadline(seed, target)
