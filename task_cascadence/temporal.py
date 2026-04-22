"""Integration helpers for the Temporal.io client."""

from __future__ import annotations

from typing import Any, Optional
from .async_utils import run_coroutine

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
        return run_coroutine(self.run_workflow(workflow, *args, **kwargs))

    async def signal_workflow(
        self,
        workflow_id: str,
        signal: str,
        *signal_args: Any,
        run_id: str | None = None,
        **signal_kwargs: Any,
    ) -> Any:
        """Signal an existing workflow execution."""
        client = await self.connect()
        workflow_handle = client.get_workflow_handle(workflow_id, run_id=run_id)
        return await workflow_handle.signal(signal, *signal_args, **signal_kwargs)

    def signal_workflow_sync(
        self,
        workflow_id: str,
        signal: str,
        *signal_args: Any,
        run_id: str | None = None,
        **signal_kwargs: Any,
    ) -> Any:
        """Synchronously signal an existing workflow execution."""
        return run_coroutine(
            self.signal_workflow(
                workflow_id,
                signal,
                *signal_args,
                run_id=run_id,
                **signal_kwargs,
            )
        )

    async def query_workflow(
        self,
        workflow_id: str,
        query: str,
        *query_args: Any,
        run_id: str | None = None,
        **query_kwargs: Any,
    ) -> Any:
        """Query an existing workflow execution."""
        client = await self.connect()
        workflow_handle = client.get_workflow_handle(workflow_id, run_id=run_id)
        return await workflow_handle.query(query, *query_args, **query_kwargs)

    def query_workflow_sync(
        self,
        workflow_id: str,
        query: str,
        *query_args: Any,
        run_id: str | None = None,
        **query_kwargs: Any,
    ) -> Any:
        """Synchronously query an existing workflow execution."""
        return run_coroutine(
            self.query_workflow(
                workflow_id,
                query,
                *query_args,
                run_id=run_id,
                **query_kwargs,
            )
        )

    def replay(self, history_path: str) -> None:
        """Replay a workflow history from ``history_path`` for debugging."""
        replayer = Replayer()  # type: ignore[call-arg]
        replayer.replay(history_path)  # type: ignore[attr-defined]
