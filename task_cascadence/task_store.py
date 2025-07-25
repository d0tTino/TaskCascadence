from __future__ import annotations

from pathlib import Path
import os
from typing import Dict, List

import yaml

from .config import load_config
from . import plugins


class TaskStore:
    """Persistent store for dynamically registered tasks."""

    def __init__(self, path: str | Path | None = None) -> None:
        if path is None:
            path = os.getenv("CASCADENCE_TASKS_PATH")
        if path is None:
            cfg = load_config()
            path = cfg.get("tasks_path")
        if path is None:
            path = Path.home() / ".cascadence" / "tasks.yml"
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: List[str] = self._load()

    def _load(self) -> List[str]:
        if self.path.exists():
            with open(self.path, "r") as fh:
                data = yaml.safe_load(fh) or []
                if isinstance(data, list):
                    return data
        return []

    def _save(self) -> None:
        with open(self.path, "w") as fh:
            yaml.safe_dump(self._data, fh)

    def add_path(self, module_path: str) -> None:
        if module_path not in self._data:
            self._data.append(module_path)
            self._save()

    def get_paths(self) -> List[str]:
        return list(self._data)

    def load_tasks(self) -> Dict[str, plugins.BaseTask]:
        tasks: Dict[str, plugins.BaseTask] = {}
        for path in self._data:
            try:
                task = plugins.load_plugin(path)
            except Exception:
                continue
            tasks[task.__class__.__name__] = task
        return tasks
