"""Synchronize PointerUpdate events via the configured transport.

Supported transports are ``grpc`` and ``nats``.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
from typing import Iterable, Any, AsyncIterator

from .config import load_config
from .pointer_store import PointerStore
from .ume.models import PointerUpdate
from .ume import emit_pointer_update, emit_audit_log
from .async_utils import run_coroutine

logger = logging.getLogger(__name__)


def _import_object(path: str) -> Any:
    module, attr = path.split(":")
    mod = importlib.import_module(module)
    return getattr(mod, attr)


async def run_async() -> None:
    """Receive ``PointerUpdate`` messages and persist them using asyncio."""

    cfg = load_config()
    transport = cfg.get("ume_transport")
    store = PointerStore()
    broadcast = bool(cfg.get("ume_broadcast_pointers"))

    if not transport:
        logger.warning("UME transport not configured. Exiting.")
        return

    async def _maybe_broadcast(update: PointerUpdate) -> None:
        if not broadcast:
            return
        try:
            user_id = update.user_id or None
            group_id = update.group_id or None
            if "use_asyncio" in inspect.signature(emit_pointer_update).parameters:
                result = emit_pointer_update(
                    update, user_id=user_id, group_id=group_id, use_asyncio=True
                )
            else:
                result = emit_pointer_update(
                    update, user_id=user_id, group_id=group_id
                )
            if isinstance(result, asyncio.Task):
                await result
        except Exception:  # pragma: no cover - best effort transport
            pass

    if transport == "grpc":
        if not cfg.get("ume_grpc_stub"):
            raise ValueError("UME_GRPC_STUB not configured")
        stub = _import_object(cfg["ume_grpc_stub"])
        method = cfg.get("ume_grpc_method", "Subscribe")
        listener = getattr(stub, method)()
        if inspect.isawaitable(listener):
            listener = await listener
        if hasattr(listener, "__aiter__"):
            async_iter = listener
        else:
            async def _gen() -> AsyncIterator[Any]:
                for item in listener:
                    yield item
            async_iter = _gen()
        async for update in async_iter:
            user_id = update.user_id or None
            group_id = update.group_id or None
            try:
                store.apply_update(update, group_id=group_id)
                await _maybe_broadcast(update)
                emit_audit_log(
                    update.task_name,
                    "pointer_sync",
                    "success",
                    user_id=user_id,
                    group_id=group_id,
                )
            except Exception as exc:  # pragma: no cover - resilient loop
                logger.exception("Error processing pointer update")
                emit_audit_log(
                    update.task_name,
                    "pointer_sync",
                    "error",
                    reason=str(exc),
                    user_id=user_id,
                    group_id=group_id,
                )
                continue
        return

    if transport == "nats":
        if not cfg.get("ume_nats_conn"):
            raise ValueError("UME_NATS_CONN not configured")
        conn = _import_object(cfg["ume_nats_conn"])
        subject = cfg.get("ume_nats_subject", "events")
        if hasattr(conn, "subscribe"):
            subscription: Iterable[Any] = conn.subscribe(subject)
        else:
            subscription = conn.subscribe_sync(subject)
        if inspect.isawaitable(subscription):
            subscription = await subscription
        if hasattr(subscription, "__aiter__"):
            async_iter = subscription
        else:
            async def _gen() -> AsyncIterator[Any]:
                for item in subscription:
                    yield item
            async_iter = _gen()
        async for msg in async_iter:
            try:
                data = getattr(msg, "data", msg)
                update = PointerUpdate()
                if isinstance(data, PointerUpdate):
                    update = data
                else:
                    update.ParseFromString(data)
                user_id = update.user_id or None
                group_id = update.group_id or None
                store.apply_update(update, group_id=group_id)
                await _maybe_broadcast(update)
                emit_audit_log(
                    update.task_name,
                    "pointer_sync",
                    "success",
                    user_id=user_id,
                    group_id=group_id,
                )
            except Exception as exc:  # pragma: no cover - resilient loop
                logger.exception("Error processing pointer update")
                try:
                    update_task_name = update.task_name  # may be unset
                except Exception:  # pragma: no cover - defensive
                    update_task_name = ""
                emit_audit_log(
                    update_task_name,
                    "pointer_sync",
                    "error",
                    reason=str(exc),
                    user_id=getattr(update, "user_id", None) or None,
                    group_id=getattr(update, "group_id", None) or None,
                )
                continue
        return

    logger.warning("Unknown UME transport: %s. Exiting.", transport)
    return


def run() -> None:
    """Synchronous wrapper for :func:`run_async`."""
    run_coroutine(run_async())


if __name__ == "__main__":  # pragma: no cover - manual execution
    run()

