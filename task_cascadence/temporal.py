"""Integration helpers for the Temporal.io client."""

from __future__ import annotations

from typing import Any, Optional
import asyncio

try:  # pragma: no cover - optional dependency
    from temporalio.client import Client
    from temporalio.worker import Replayer
except Exception:  # pragma: no cover - library not installed
    Client = None  # type: ignore
    Replayer = None  # type: ignore


class TemporalBackend:
    """Thin wrapper around :class:`temporalio.client.Client`."""

    def __init__(self, server: str = "localhost:7233") -> None:
        self.server = server
        self._client: Optional[Client] = None

    async def connect(self) -> Client:
        if Client is None:
            raise RuntimeError("temporalio is not installed")
        if not self._client:
            self._client = await Client.connect(self.server)
        return self._client

    async def run_workflow(self, workflow: str, *args: Any, **kwargs: Any) -> Any:
        client = await self.connect()
        return await client.execute_workflow(workflow, *args, **kwargs)

    def run_workflow_sync(self, workflow: str, *args: Any, **kwargs: Any) -> Any:
        """Synchronously execute ``workflow`` and return its result."""
        return asyncio.run(self.run_workflow(workflow, *args, **kwargs))

    def replay(self, history_path: str) -> None:
        """Replay a workflow history from ``history_path`` for debugging."""
        if Replayer is None:
            raise RuntimeError("temporalio is not installed")
        Replayer().replay(history_path)
