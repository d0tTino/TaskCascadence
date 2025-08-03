from __future__ import annotations

from pathlib import Path
import os
from typing import Any, Dict, List

from filelock import FileLock

import yaml

from .ume import _hash_user_id, emit_pointer_update
from .ume.models import PointerUpdate
from .config import load_config


class PointerStore:
    """Persistent store for :class:`~task_cascadence.plugins.PointerTask` pointers."""

    def __init__(self, path: str | Path | None = None) -> None:
        if path is None:
            path = os.getenv("CASCADENCE_POINTERS_PATH")
        if path is None:
            cfg = load_config()
            path = cfg.get("pointers_path")
        if path is None:
            path = Path.home() / ".cascadence" / "pointers.yml"
        self.path = Path(path)
        self._lock = FileLock(str(self.path) + ".lock")
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
        with self._lock:
            current = self._load()
            for task, entries in current.items():
                existing = self._data.setdefault(task, [])
                for entry in entries:
                    if entry not in existing:
                        existing.append(entry)

            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            with open(tmp, "w") as fh:
                yaml.safe_dump(self._data, fh)
            os.replace(tmp, self.path)

    def add_pointer(self, task_name: str, user_id: str, run_id: str) -> None:
        user_hash = _hash_user_id(user_id)
        entry = {"run_id": run_id, "user_hash": user_hash}
        pointers = self._data.setdefault(task_name, [])
        if any(e == entry for e in pointers):
            return

        pointers.append(entry)
        self._save()
        try:
            emit_pointer_update(
                PointerUpdate(task_name=task_name, run_id=run_id, user_hash=user_hash),
                user_id=user_id,
            )
        except Exception:  # pragma: no cover - best effort transport
            pass

    def apply_update(self, update: PointerUpdate) -> None:
        entry = {"run_id": update.run_id, "user_hash": update.user_hash}
        pointers = self._data.setdefault(update.task_name, [])
        if any(e == entry for e in pointers):
            return

        pointers.append(entry)
        self._save()

    def get_pointers(self, task_name: str) -> List[Dict[str, Any]]:
        return list(self._data.get(task_name, []))
