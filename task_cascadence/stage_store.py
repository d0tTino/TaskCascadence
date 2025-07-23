from __future__ import annotations

from pathlib import Path
import os
from typing import Any, Dict, List

import yaml


class StageStore:
    """Persistent store for pipeline stage events."""

    def __init__(self, path: str | Path | None = None) -> None:
        if path is None:
            path = os.getenv("CASCADENCE_STAGES_PATH")
        if path is None:
            path = Path.home() / ".cascadence" / "stages.yml"
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

    def add_event(self, task_name: str, stage: str, user_hash: str | None) -> None:
        entry: Dict[str, Any] = {"stage": stage}
        if user_hash is not None:
            entry["user_hash"] = user_hash
        events = self._data.setdefault(task_name, [])
        events.append(entry)
        self._save()

    def get_events(self, task_name: str) -> List[Dict[str, Any]]:
        return list(self._data.get(task_name, []))
