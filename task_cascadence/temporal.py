"""Integration helpers for the Temporal.io client."""

from __future__ import annotations

from typing import Any, Optional
import asyncio

from temporalio.client import Client
from temporalio.worker import Replayer


class TemporalBackend:
    """Thin wrapper around :class:`temporalio.client.Client`."""

    def __init__(self, server: str | None = None) -> None:
        if server is None:
            from .config import load_config

            server = load_config().get("temporal_server", "localhost:7233")
        self.server = server
        self._client: Optional[Client] = None

    async def connect(self) -> Client:
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
        Replayer().replay(history_path)
