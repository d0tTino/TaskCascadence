"""Configuration helpers for Cascadence."""

from __future__ import annotations

import os
from typing import Any, Dict

import yaml


def load_config(path: str | None = None) -> Dict[str, Any]:
    """Load configuration from ``path`` or ``CASCADENCE_CONFIG`` env var.

    The configuration currently supports selecting the scheduler backend via the
    ``scheduler`` key. Environment variable ``CASCADENCE_SCHEDULER`` overrides
    any value found in the YAML file. If no configuration is provided the
    scheduler defaults to ``cron``.
    """

    cfg: Dict[str, Any] = {}
    path = path or os.getenv("CASCADENCE_CONFIG")
    if path and os.path.exists(path):
        with open(path, "r") as fh:
            cfg = yaml.safe_load(fh) or {}
    scheduler = os.getenv("CASCADENCE_SCHEDULER", cfg.get("scheduler", "cron"))
    cfg["scheduler"] = scheduler
    return cfg

