"""Helpers for task research using the optional ``tino_storm`` package."""

from __future__ import annotations

from typing import Any
import inspect

try:
    import tino_storm  # type: ignore
except Exception:  # pragma: no cover - optional dependency may be missing
    tino_storm = None  # type: ignore
def gather(query: str, user_id: str, group_id: str | None = None) -> Any:
    """Return research information for ``query`` using ``tino_storm``.

    Parameters are forwarded to ``tino_storm`` if supported.
    """
    if tino_storm is None:  # pragma: no cover - runtime behaviour
        raise RuntimeError(f"tino_storm is not installed for user {user_id}")

    if hasattr(tino_storm, "search"):
        func = tino_storm.search
    elif callable(tino_storm):
        func = tino_storm
    else:
        raise RuntimeError(f"Unsupported tino_storm interface for user {user_id}")

    params = inspect.signature(func).parameters
    kwargs: dict[str, Any] = {}
    if "user_id" in params:
        kwargs["user_id"] = user_id
    if "group_id" in params and group_id is not None:
        kwargs["group_id"] = group_id
    return func(query, **kwargs)


async def async_gather(
    query: str, user_id: str, group_id: str | None = None
) -> Any:
    """Asynchronously return research information for ``query`` using ``tino_storm``.

    If the call to ``tino_storm`` returns a coroutine it will be awaited.
    """
    if tino_storm is None:  # pragma: no cover - runtime behaviour
        raise RuntimeError(f"tino_storm is not installed for user {user_id}")

    if hasattr(tino_storm, "search"):
        func = tino_storm.search
    elif callable(tino_storm):
        func = tino_storm
    else:
        raise RuntimeError(f"Unsupported tino_storm interface for user {user_id}")

    params = inspect.signature(func).parameters
    kwargs: dict[str, Any] = {}
    if "user_id" in params:
        kwargs["user_id"] = user_id
    if "group_id" in params and group_id is not None:
        kwargs["group_id"] = group_id

    result = func(query, **kwargs)
    if inspect.isawaitable(result):
        return await result
    return result
