"""Async helpers for running coroutines in a loop-aware manner."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Coroutine


def run_coroutine(coro: Coroutine[Any, Any, Any]) -> Any:
    """Synchronously execute *coro* and return its result.

    When no event loop is running ``asyncio.run`` is used. If a loop is already
    running (e.g. when called from synchronous code within an async test) the
    coroutine is executed in a dedicated thread with its own event loop so the
    current loop is not blocked.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    else:
        with ThreadPoolExecutor(max_workers=1) as executor:
            return executor.submit(lambda: asyncio.run(coro)).result()
