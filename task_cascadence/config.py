"""Configuration helpers for Cascadence."""

from __future__ import annotations

import os
from typing import Any, Dict

import yaml


def load_config(path: str | None = None) -> Dict[str, Any]:
    """Load configuration from ``path`` or ``CASCADENCE_CONFIG`` env var.

    The configuration currently supports selecting the scheduler backend via the
    ``backend`` key (``scheduler`` is accepted for backwards compatibility).
    Environment variable ``CASCADENCE_SCHEDULER`` overrides any value found in
    the YAML file. If no configuration is provided the scheduler defaults to
    ``cron``.
    """

    cfg: Dict[str, Any] = {}
    path = path or os.getenv("CASCADENCE_CONFIG")
    if path and os.path.exists(path):
        with open(path, "r") as fh:
            cfg = yaml.safe_load(fh) or {}
    backend = os.getenv(
        "CASCADENCE_SCHEDULER", cfg.get("backend", cfg.get("scheduler", "cron"))
    )
    cfg["backend"] = backend

    refresh_env = os.getenv("CASCADENCE_CRONYX_REFRESH")
    if refresh_env is not None:
        cfg["cronyx_refresh"] = refresh_env.lower() not in ("0", "false", "no")
    else:
        cfg["cronyx_refresh"] = bool(cfg.get("cronyx_refresh", True))

    return cfg

