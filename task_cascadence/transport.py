"""Transport clients for delivering events to external services."""

from __future__ import annotations

from typing import Any


class BaseTransport:
    """Abstract transport client interface."""

    def enqueue(self, obj: Any, timeout: float = 0.2) -> None:
        """Send *obj* using the transport within ``timeout`` seconds."""
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


def get_client(transport: str, **kwargs: Any) -> BaseTransport:
    """Return a transport client for ``transport``."""
    if transport == "grpc":
        return GrpcClient(**kwargs)
    if transport == "nats":
        return NatsClient(**kwargs)
    raise ValueError(f"Unknown transport type: {transport}")


__all__ = [
    "BaseTransport",
    "GrpcClient",
    "NatsClient",
    "get_client",
]
