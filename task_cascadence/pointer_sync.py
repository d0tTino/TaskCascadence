"""Synchronize PointerUpdate events via the configured transport.

Supported transports are ``grpc`` and ``nats``.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
from typing import Iterable, Any

from .config import load_config
from .pointer_store import PointerStore
from .ume.models import PointerUpdate
from .ume import emit_pointer_update

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
        print("UME transport not configured. Exiting.")
        return

    async def _maybe_broadcast(update: PointerUpdate) -> None:
        if not broadcast:
            return
        try:
            if "use_asyncio" in inspect.signature(emit_pointer_update).parameters:
                result = emit_pointer_update(update, use_asyncio=True)
            else:
                result = emit_pointer_update(update)
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
            async def _gen():
                for item in listener:
                    yield item
            async_iter = _gen()
        async for update in async_iter:
            try:
                store.apply_update(update)
                await _maybe_broadcast(update)
            except Exception:  # pragma: no cover - resilient loop
                logger.exception("Error processing pointer update")
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
            async def _gen():
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
                store.apply_update(update)
                await _maybe_broadcast(update)
            except Exception:  # pragma: no cover - resilient loop
                logger.exception("Error processing pointer update")
                continue
        return

    print(f"Unknown UME transport: {transport}. Exiting.")
    return


def run() -> None:
    """Synchronous wrapper for :func:`run_async`."""

    asyncio.run(run_async())


if __name__ == "__main__":  # pragma: no cover - manual execution
    run()

