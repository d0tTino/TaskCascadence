"""Cascadence package root.

This package provides task orchestration utilities described in the PRD.
"""

from . import scheduler  # noqa: F401
from . import plugins  # noqa: F401
from . import ume  # noqa: F401
from . import cli  # noqa: F401
from . import metrics  # noqa: F401
from . import transport  # noqa: F401


def _load_remote_tasks() -> None:
    """Load tasks from a CronyxServer if ``CRONYX_BASE_URL`` is set."""

    import os
    from importlib import import_module

    base_url = os.getenv("CRONYX_BASE_URL")
    if not base_url:
        return

    from .plugins.cronyx_server import CronyxServerLoader

    loader = CronyxServerLoader(base_url)

    for entry in loader.list_tasks():
        task_id = entry["id"] if isinstance(entry, dict) else entry
        spec = loader.load_task(task_id)
        path = spec.get("path") or spec.get("module")
        if not path:
            continue
        module_name, class_name = path.split(":", 1)
        module = import_module(module_name)
        cls = getattr(module, class_name)
        name = spec.get("name", getattr(cls, "name", task_id))
        scheduler.default_scheduler.register_task(name, cls())


_load_remote_tasks()

__all__ = ["scheduler", "plugins", "ume", "cli", "metrics"]
