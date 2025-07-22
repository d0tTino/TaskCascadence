"""Transport clients for delivering events to external services."""

from __future__ import annotations

from typing import Any


class BaseTransport:
    """Abstract transport client interface."""

    def enqueue(self, obj: Any, timeout: float = 0.2) -> None:
        """Send *obj* using the transport within ``timeout`` seconds."""
        raise NotImplementedError


class AsyncBaseTransport:
    """Abstract asynchronous transport client interface."""

    async def enqueue(self, obj: Any, timeout: float = 0.2) -> None:
        """Asynchronously send *obj* within ``timeout`` seconds."""
        raise NotImplementedError


class GrpcClient(BaseTransport):
    """gRPC transport client using a provided stub."""

    def __init__(self, stub: Any, method: str = "Send") -> None:
        self._stub = stub
        self._method = method

    def enqueue(self, obj: Any, timeout: float = 0.2) -> None:  # pragma: no cover - simple delegation
        rpc = getattr(self._stub, self._method)
        rpc(obj, timeout=timeout)


class NatsClient(BaseTransport):
    """NATS transport client using a connection object."""

    def __init__(self, connection: Any, subject: str = "events") -> None:
        self._connection = connection
        self._subject = subject

    def enqueue(self, obj: Any, timeout: float = 0.2) -> None:  # pragma: no cover - simple delegation
        self._connection.publish(self._subject, obj)
        self._connection.flush(timeout=timeout)


class AsyncGrpcClient(AsyncBaseTransport):
    """Asynchronous gRPC transport using an async stub."""

    def __init__(self, stub: Any, method: str = "Send") -> None:
        self._stub = stub
        self._method = method

    async def enqueue(self, obj: Any, timeout: float = 0.2) -> None:  # pragma: no cover - simple delegation
        rpc = getattr(self._stub, self._method)
        await rpc(obj, timeout=timeout)


class AsyncNatsClient(AsyncBaseTransport):
    """Asynchronous NATS transport using an async connection."""

    def __init__(self, connection: Any, subject: str = "events") -> None:
        self._connection = connection
        self._subject = subject

    async def enqueue(self, obj: Any, timeout: float = 0.2) -> None:  # pragma: no cover - simple delegation
        await self._connection.publish(self._subject, obj)
        await self._connection.flush(timeout=timeout)


def get_client(transport: str, **kwargs: Any) -> BaseTransport | AsyncBaseTransport:
    """Return a transport client for ``transport``."""
    if transport == "grpc":
        return GrpcClient(**kwargs)
    if transport == "nats":
        return NatsClient(**kwargs)
    if transport == "grpc_async":
        return AsyncGrpcClient(**kwargs)
    if transport == "nats_async":
        return AsyncNatsClient(**kwargs)
    raise ValueError(f"Unknown transport type: {transport}")


__all__ = [
    "BaseTransport",
    "AsyncBaseTransport",
    "GrpcClient",
    "NatsClient",
    "AsyncGrpcClient",
    "AsyncNatsClient",
    "get_client",
]
