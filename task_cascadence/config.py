"""Configuration helpers for Cascadence."""

from __future__ import annotations

import os
from typing import Any, Dict

import yaml


def load_config(path: str | None = None) -> Dict[str, Any]:
    """Load configuration from ``path`` or ``CASCADENCE_CONFIG`` env var.

    The configuration currently supports selecting the scheduler backend via the
    ``backend`` key (``scheduler`` is accepted for backwards compatibility).
    ``backend`` may be ``cron``, ``base``, ``temporal`` or ``cronyx``.
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

    timezone = os.getenv("CASCADENCE_TIMEZONE", cfg.get("timezone", "UTC"))
    cfg["timezone"] = timezone

    refresh_env = os.getenv("CASCADENCE_CRONYX_REFRESH")
    if refresh_env is not None:
        cfg["cronyx_refresh"] = refresh_env.lower() not in ("0", "false", "no")
    else:
        cfg["cronyx_refresh"] = bool(cfg.get("cronyx_refresh", True))

    if "CRONYX_BASE_URL" in os.environ:
        cfg["cronyx_base_url"] = os.environ["CRONYX_BASE_URL"]
    if "CRONYX_TIMEOUT" in os.environ:
        cfg["cronyx_timeout"] = float(os.environ["CRONYX_TIMEOUT"])

    if "TEMPORAL_SERVER" in os.environ:
        cfg["temporal_server"] = os.environ["TEMPORAL_SERVER"]

    if "UME_TRANSPORT" in os.environ:
        cfg["ume_transport"] = os.environ["UME_TRANSPORT"]
    if "UME_GRPC_STUB" in os.environ:
        cfg["ume_grpc_stub"] = os.environ["UME_GRPC_STUB"]
    if "UME_GRPC_METHOD" in os.environ:
        cfg["ume_grpc_method"] = os.environ["UME_GRPC_METHOD"]
    if "UME_NATS_CONN" in os.environ:
        cfg["ume_nats_conn"] = os.environ["UME_NATS_CONN"]
    if "UME_NATS_SUBJECT" in os.environ:
        cfg["ume_nats_subject"] = os.environ["UME_NATS_SUBJECT"]
    if "UME_BROADCAST_POINTERS" in os.environ:
        cfg["ume_broadcast_pointers"] = os.environ["UME_BROADCAST_POINTERS"].lower() not in (
            "0",
            "false",
            "no",
        )

    if "CASCADENCE_HASH_SECRET" in os.environ:
        cfg["hash_secret"] = os.environ["CASCADENCE_HASH_SECRET"]
    else:
        cfg.setdefault("hash_secret", "")

    if "CASCADENCE_STAGES_PATH" in os.environ:
        cfg["stages_path"] = os.environ["CASCADENCE_STAGES_PATH"]
    elif "stages_path" in cfg:
        cfg["stages_path"] = cfg["stages_path"]

    if "CASCADENCE_POINTERS_PATH" in os.environ:
        cfg["pointers_path"] = os.environ["CASCADENCE_POINTERS_PATH"]
    elif "pointers_path" in cfg:
        cfg["pointers_path"] = cfg["pointers_path"]

    if "ume_broadcast_pointers" in cfg:
        cfg["ume_broadcast_pointers"] = bool(cfg["ume_broadcast_pointers"])

    return cfg

