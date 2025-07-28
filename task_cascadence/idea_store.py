from __future__ import annotations

from pathlib import Path
import os
import uuid
from typing import Dict

import yaml

from .config import load_config


class IdeaStore:
    """Persistent store for submitted ideas."""

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
        self._data: Dict[str, str] = self._load()

    def _load(self) -> Dict[str, str]:
        if self.path.exists():
            with open(self.path, "r") as fh:
                data = yaml.safe_load(fh) or {}
                if isinstance(data, dict):
                    return {str(k): str(v) for k, v in data.items()}
        return {}

    def _save(self) -> None:
        with open(self.path, "w") as fh:
            yaml.safe_dump(self._data, fh)

    def add_idea(self, idea: str) -> str:
        idea_id = uuid.uuid4().hex
        self._data[idea_id] = idea
        self._save()
        return idea_id

    def get_idea(self, idea_id: str) -> str | None:
        return self._data.get(idea_id)
