from __future__ import annotations

from pathlib import Path
import os
from typing import Any, Dict, List
from datetime import datetime, timezone

import yaml

from .config import load_config


class SuggestionStore:
    """Persistent store for suggestion decisions."""

    def __init__(self, path: str | Path | None = None) -> None:
        if path is None:
            path = os.getenv("CASCADENCE_SUGGESTIONS_PATH")
        if path is None:
            cfg = load_config()
            path = cfg.get("suggestions_path")
        if path is None:
            path = Path.home() / ".cascadence" / "suggestions.yml"
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: List[Dict[str, Any]] = self._load()

    def _load(self) -> List[Dict[str, Any]]:
        if self.path.exists():
            with open(self.path, "r") as fh:
                data = yaml.safe_load(fh) or []
                if isinstance(data, list):
                    return data
        return []

    def _save(self) -> None:
        with open(self.path, "w") as fh:
            yaml.safe_dump(self._data, fh)

    def add_decision(
        self,
        pattern: str,
        decision: str,
        user_hash: str | None,
        group_id: str | None = None,
    ) -> None:
        entry: Dict[str, Any] = {
            "pattern": pattern,
            "decision": decision,
            "time": datetime.now(timezone.utc).isoformat(),
        }
        if user_hash is not None:
            entry["user_hash"] = user_hash
        if group_id is not None:
            entry["group_id"] = group_id
        self._data.append(entry)
        self._save()

    def get_decisions(
        self, user_hash: str | None = None, group_id: str | None = None
    ) -> List[Dict[str, Any]]:
        return [
            d
            for d in self._data
            if (user_hash is None or d.get("user_hash") == user_hash)
            and (group_id is None or d.get("group_id") == group_id)
        ]


__all__ = ["SuggestionStore"]

