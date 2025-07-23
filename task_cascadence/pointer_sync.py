"""Synchronize PointerUpdate events via the configured transport."""

from __future__ import annotations

import importlib
from typing import Iterable, Any

from .config import load_config
from .pointer_store import PointerStore
from .ume.models import PointerUpdate


def _import_object(path: str) -> Any:
    module, attr = path.split(":")
    mod = importlib.import_module(module)
    return getattr(mod, attr)


def run() -> None:
    """Receive ``PointerUpdate`` messages and persist them."""

    cfg = load_config()
    transport = cfg.get("ume_transport")
    store = PointerStore()

    if transport == "grpc":
        if not cfg.get("ume_grpc_stub"):
            raise ValueError("UME_GRPC_STUB not configured")
        stub = _import_object(cfg["ume_grpc_stub"])
        method = cfg.get("ume_grpc_method", "Subscribe")
        listener = getattr(stub, method)()
        for update in listener:
            store.apply_update(update)
        return

    if transport == "nats":
        if not cfg.get("ume_nats_conn"):
            raise ValueError("UME_NATS_CONN not configured")
        conn = _import_object(cfg["ume_nats_conn"])
        subject = cfg.get("ume_nats_subject", "events")
        subscription: Iterable[Any] = conn.subscribe_sync(subject)
        for msg in subscription:
            data = getattr(msg, "data", msg)
            update = PointerUpdate()
            if isinstance(data, PointerUpdate):
                update = data
            else:
                update.ParseFromString(data)
            store.apply_update(update)
        return

    raise ValueError("Unknown or unset UME transport")


if __name__ == "__main__":  # pragma: no cover - manual execution
    run()

