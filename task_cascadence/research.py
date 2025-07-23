"""Helpers for task research using the optional ``tino_storm`` package."""

from __future__ import annotations

from typing import Any

try:
    import tino_storm  # type: ignore
except Exception:  # pragma: no cover - optional dependency may be missing
    tino_storm = None  # type: ignore


def gather(query: str) -> Any:
    """Return research information for ``query`` using ``tino_storm``."""
    if tino_storm is None:  # pragma: no cover - runtime behaviour
        raise RuntimeError("tino_storm is not installed")

    if hasattr(tino_storm, "search"):
        return tino_storm.search(query)

    if callable(tino_storm):
        return tino_storm(query)

    raise RuntimeError("Unsupported tino_storm interface")
