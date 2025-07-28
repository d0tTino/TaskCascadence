"""Synchronize PointerUpdate events via the configured transport.

Supported transports are ``grpc`` and ``nats``.
"""

from __future__ import annotations

import importlib
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


def run() -> None:
    """Receive ``PointerUpdate`` messages and persist them.

    Supported transports are ``grpc`` and ``nats``.
    """

    cfg = load_config()
    transport = cfg.get("ume_transport")
    store = PointerStore()
    broadcast = bool(cfg.get("ume_broadcast_pointers"))

    if not transport:
        print("UME transport not configured. Exiting.")
        return

    def _maybe_broadcast(update: PointerUpdate) -> None:
        if not broadcast:
            return
        try:
            emit_pointer_update(update)
        except Exception:  # pragma: no cover - best effort transport
            pass

    if transport == "grpc":
        if not cfg.get("ume_grpc_stub"):
            raise ValueError("UME_GRPC_STUB not configured")
        stub = _import_object(cfg["ume_grpc_stub"])
        method = cfg.get("ume_grpc_method", "Subscribe")
        listener = getattr(stub, method)()
        for update in listener:
            try:
                store.apply_update(update)
                _maybe_broadcast(update)
            except Exception:  # pragma: no cover - resilient loop
                logger.exception("Error processing pointer update")
                continue
        return

    if transport == "nats":
        if not cfg.get("ume_nats_conn"):
            raise ValueError("UME_NATS_CONN not configured")
        conn = _import_object(cfg["ume_nats_conn"])
        subject = cfg.get("ume_nats_subject", "events")
        subscription: Iterable[Any] = conn.subscribe_sync(subject)
        for msg in subscription:
            try:
                data = getattr(msg, "data", msg)
                update = PointerUpdate()
                if isinstance(data, PointerUpdate):
                    update = data
                else:
                    update.ParseFromString(data)
                store.apply_update(update)
                _maybe_broadcast(update)
            except Exception:  # pragma: no cover - resilient loop
                logger.exception("Error processing pointer update")
                continue
        return

    print(f"Unknown UME transport: {transport}. Exiting.")
    return


if __name__ == "__main__":  # pragma: no cover - manual execution
    run()

