"""Async helpers for running coroutines in a loop-aware manner."""

from __future__ import annotations

import asyncio
from typing import Any, Coroutine


def run_coroutine(coro: Coroutine[Any, Any, Any]) -> Any:
    """Run ``coro`` using the current event loop if available.

    If no event loop is running, a temporary loop is created and closed after
    execution. When a loop is running, the coroutine is scheduled using
    :func:`asyncio.create_task` and the resulting task is returned for the
    caller to await.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro)
        finally:
            asyncio.set_event_loop(None)
            loop.close()
    else:
        return loop.create_task(coro)
