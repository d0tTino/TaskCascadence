from __future__ import annotations

from pathlib import Path
import os
from typing import Any, Dict, List

import yaml

from .ume import _hash_user_id


class PointerStore:
    """Persistent store for :class:`~task_cascadence.plugins.PointerTask` pointers."""

    def __init__(self, path: str | Path | None = None) -> None:
        if path is None:
            path = os.getenv("CASCADENCE_POINTERS_PATH")
        if path is None:
            path = Path.home() / ".cascadence" / "pointers.yml"
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: Dict[str, List[Dict[str, Any]]] = self._load()

    def _load(self) -> Dict[str, List[Dict[str, Any]]]:
        if self.path.exists():
            with open(self.path, "r") as fh:
                data = yaml.safe_load(fh) or {}
                if isinstance(data, dict):
                    return data
        return {}

    def _save(self) -> None:
        with open(self.path, "w") as fh:
            yaml.safe_dump(self._data, fh)

    def add_pointer(self, task_name: str, user_id: str, run_id: str) -> None:
        entry = {"run_id": run_id, "user_hash": _hash_user_id(user_id)}
        self._data.setdefault(task_name, []).append(entry)
        self._save()

    def get_pointers(self, task_name: str) -> List[Dict[str, Any]]:
        return list(self._data.get(task_name, []))
