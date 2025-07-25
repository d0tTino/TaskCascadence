from __future__ import annotations

from pathlib import Path
import os
from typing import Any, Dict, List
from datetime import datetime, timezone

import yaml

from .config import load_config


class StageStore:
    """Persistent store for pipeline stage events."""

    def __init__(self, path: str | Path | None = None) -> None:
        if path is None:
            path = os.getenv("CASCADENCE_STAGES_PATH")
        if path is None:
            cfg = load_config()
            path = cfg.get("stages_path")
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
                    for key, events in data.items():
                        if isinstance(events, list):
                            normalized = []
                            for event in events:
                                if isinstance(event, dict):
                                    normalized.append(event)
                                else:
                                    # legacy string entry
                                    normalized.append({"stage": str(event)})
                            data[key] = normalized
                    return data
        return {}

    def _save(self) -> None:
        """Persist data to disk with an exclusive file lock."""
        mode = "r+" if self.path.exists() else "w+"
        with open(self.path, mode) as fh:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(fh.fileno(), msvcrt.LK_LOCK, 1)  # type: ignore[attr-defined]
            else:
                import fcntl

                fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            try:
                fh.seek(0)
                yaml.safe_dump(self._data, fh)
                fh.truncate()
                fh.flush()
            finally:
                if os.name == "nt":
                    msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]
                else:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

    def add_event(self, task_name: str, stage: str, user_hash: str | None) -> None:
        entry: Dict[str, Any] = {
            "stage": stage,
            "time": datetime.now(timezone.utc).isoformat(),
        }
        if user_hash is not None:
            entry["user_hash"] = user_hash
        events = self._data.setdefault(task_name, [])
        events.append(entry)
        self._save()

    def get_events(self, task_name: str) -> List[Dict[str, Any]]:
        return list(self._data.get(task_name, []))
