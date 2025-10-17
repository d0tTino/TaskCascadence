from __future__ import annotations

from pathlib import Path
import os
from typing import Any, Dict, List, Type, cast
from threading import RLock
from datetime import datetime, timezone

import yaml

try:  # pragma: no cover - depends on optional libyaml acceleration
    _YAML_DUMPER: Type[yaml.SafeDumper] = cast(  # type: ignore[misc]
        Type[yaml.SafeDumper], yaml.CSafeDumper
    )
except AttributeError:  # pragma: no cover - fallback when C bindings missing
    _YAML_DUMPER = yaml.SafeDumper

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
        self.path.touch(exist_ok=True)
        self._lock = RLock()
        self._data: Dict[str, List[Dict[str, Any]]] = self._load()

    def _load(self) -> Dict[str, List[Dict[str, Any]]]:
        if self.path.exists():
            with open(self.path, "r") as fh:
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(fh.fileno(), msvcrt.LK_LOCK, 1)  # type: ignore[attr-defined]
                else:
                    import fcntl

                    fcntl.flock(fh.fileno(), fcntl.LOCK_SH)
                try:
                    data = yaml.safe_load(fh) or {}
                finally:
                    if os.name == "nt":
                        msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]
                    else:
                        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                if isinstance(data, dict):
                    for key, events in data.items():
                        if isinstance(events, list):
                            normalized = []
                            for event in events:
                                if isinstance(event, dict):
                                    # migrate legacy "user_id" field
                                    if "user_id" in event and "user_hash" not in event:
                                        event["user_hash"] = event.pop("user_id")
                                    normalized.append(event)
                                else:
                                    # legacy string entry
                                    normalized.append({"stage": str(event)})
                            data[key] = normalized
                    return data
        return {}

    def _save(self) -> None:
        """Persist data to disk with an exclusive file lock."""
        with self._lock:
            self._save_locked()

    def _save_locked(self) -> None:
        rendered = yaml.dump(
            self._data,
            Dumper=_YAML_DUMPER,
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
        )
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
                fh.write(rendered)
                fh.truncate()
                fh.flush()
            finally:
                if os.name == "nt":
                    msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)  # type: ignore[attr-defined]
                else:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

    def add_event(
        self,
        task_name: str,
        stage: str,
        user_hash: str | None,
        group_id: str | None = None,
        *,
        status: str | None = None,
        reason: str | None = None,
        output: str | None = None,
        partial: Any | None = None,
        category: str = "stage",
        run_id: str | None = None,
    ) -> None:
        entry: Dict[str, Any] = {
            "stage": stage,
            "time": datetime.now(timezone.utc).isoformat(),
        }
        if status is not None:
            entry["status"] = status
        if reason is not None:
            entry["reason"] = reason
        if output is not None:
            entry["output"] = output
        if partial is not None:
            entry["partial"] = partial
        if user_hash is not None:
            entry["user_hash"] = user_hash
        if group_id is not None:
            entry["group_id"] = group_id
        if run_id is not None:
            entry["run_id"] = run_id
        key = task_name if category == "stage" else f"{task_name}:{category}"
        with self._lock:
            events = self._data.setdefault(key, [])
            events.append(entry)
            self._save_locked()

    def get_events(
        self,
        task_name: str,
        user_hash: str | None = None,
        group_id: str | None = None,
        *,
        category: str = "stage",
    ) -> List[Dict[str, Any]]:
        key = task_name if category == "stage" else f"{task_name}:{category}"
        events = self._data.get(key, [])
        return [
            e
            for e in events
            if (user_hash is None or e.get("user_hash") == user_hash)
            and (group_id is None or e.get("group_id") == group_id)
        ]
