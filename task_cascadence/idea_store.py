from __future__ import annotations

from pathlib import Path
import os
from typing import Any, Dict, List

import yaml

from .config import load_config
from .ume.models import IdeaSeed


class IdeaStore:
    """Persistent store for :class:`IdeaSeed` objects."""

    def __init__(self, path: str | Path | None = None) -> None:
        if path is None:
            path = os.getenv("CASCADENCE_IDEAS_PATH")
        if path is None:
            cfg = load_config()
            path = cfg.get("ideas_path")
        if path is None:
            path = Path.home() / ".cascadence" / "ideas.yml"
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

    def add_seed(self, seed: IdeaSeed) -> None:
        entry = {"text": seed.text}
        user_hash = getattr(seed, "user_hash", None)
        if user_hash:
            entry["user_hash"] = user_hash
        self._data.append(entry)
        self._save()

    def get_seeds(self) -> List[Dict[str, Any]]:
        return list(self._data)

__all__ = ["IdeaStore"]
