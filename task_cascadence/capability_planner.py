"""Plan follow-up actions based on failed task runs."""

from __future__ import annotations

import importlib
import logging
from typing import Iterable, Any

from .config import load_config
from .scheduler import get_default_scheduler
from .ume.models import TaskRun
from .plugins import CronTask

logger = logging.getLogger(__name__)


class _FollowUpTask(CronTask):
    """Placeholder task scheduled for failed runs."""

    name = "capability_follow_up"

    def run(self):  # pragma: no cover - trivial demo
        pass


def _import_object(path: str) -> Any:
    module, attr = path.split(":")
    mod = importlib.import_module(module)
    return getattr(mod, attr)


def run() -> None:
    """Watch UME for ``TaskRun`` events with ``status='error'``."""

    cfg = load_config()
    transport = cfg.get("ume_transport")
    sched = get_default_scheduler()

    if not transport:
        print("UME transport not configured. Exiting.")
        return

    def _schedule_follow_up() -> None:
        try:
            sched.register_task(_FollowUpTask.name, _FollowUpTask())
        except Exception:  # pragma: no cover - resilient loop
            logger.exception("Error scheduling follow-up task")

    if transport == "grpc":
        if not cfg.get("ume_grpc_stub"):
            raise ValueError("UME_GRPC_STUB not configured")
        stub = _import_object(cfg["ume_grpc_stub"])
        method = cfg.get("ume_grpc_method", "Subscribe")
        listener = getattr(stub, method)()
        for event in listener:
            try:
                if getattr(event, "status", "") == "error":
                    _schedule_follow_up()
            except Exception:  # pragma: no cover - resilient loop
                logger.exception("Error processing run event")
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
                event = TaskRun()
                if isinstance(data, TaskRun):
                    event = data
                else:
                    event.ParseFromString(data)
                if event.status == "error":
                    _schedule_follow_up()
            except Exception:  # pragma: no cover - resilient loop
                logger.exception("Error processing run event")
                continue
        return

    print(f"Unknown UME transport: {transport}. Exiting.")
    return


if __name__ == "__main__":  # pragma: no cover - manual execution
    run()
