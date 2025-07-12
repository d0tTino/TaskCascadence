"""Simple in-memory scheduler.

This module provides a minimal scheduler implementation that mimics the
behaviour described in the PRD.  It is intentionally lightweight so the CLI
can interact with tasks without pulling in heavy dependencies like
APScheduler.
"""


from typing import Any, Dict, Iterable, Tuple


class BaseScheduler:
    """Very small task scheduler used by the CLI."""

    def __init__(self) -> None:
        self._tasks: Dict[str, Dict[str, Any]] = {}

    def register_task(self, name: str, task: Any) -> None:
        """Register a task object under ``name``."""

        self._tasks[name] = {"task": task, "disabled": False}

    # ------------------------------------------------------------------
    # Query helpers
    def list_tasks(self) -> Iterable[Tuple[str, bool]]:
        """Return an iterable of ``(name, disabled)`` tuples."""

        for name, info in self._tasks.items():
            yield name, info["disabled"]

    def run_task(self, name: str) -> Any:
        """Run a task by name if it exists and is enabled."""

        info = self._tasks.get(name)
        if not info:
            raise ValueError(f"Unknown task: {name}")
        if info["disabled"]:
            raise ValueError(f"Task '{name}' is disabled")
        task = info["task"]
        if hasattr(task, "run"):
            return task.run()
        raise AttributeError(f"Task '{name}' has no run() method")

    def disable_task(self, name: str) -> None:
        """Disable a registered task."""

        if name not in self._tasks:
            raise ValueError(f"Unknown task: {name}")
        self._tasks[name]["disabled"] = True


# ``default_scheduler`` is used by the CLI.  Tasks from
# :mod:`task_cascadence.plugins` will register themselves with it.
default_scheduler = BaseScheduler()
