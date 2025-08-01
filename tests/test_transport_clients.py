import asyncio
import pytest

from task_cascadence.transport import (
    BaseTransport,
    AsyncBaseTransport,
    GrpcClient,
    NatsClient,
    AsyncGrpcClient,
    AsyncNatsClient,
    get_client,
)


class DummyStub:
    def __init__(self):
        self.called = []

    def Send(self, msg, timeout=None):
        self.called.append((msg, timeout))


class DummyAsyncStub:
    def __init__(self):
        self.called = []

    async def Send(self, msg, timeout=None):
        self.called.append((msg, timeout))


class DummyConn:
    def __init__(self):
        self.actions = []

    def publish(self, subject, msg):
        self.actions.append(("pub", subject, msg))

    def flush(self, timeout=0):
        self.actions.append(("flush", timeout))


class DummyAsyncConn:
    def __init__(self):
        self.actions = []

    async def publish(self, subject, msg):
        self.actions.append(("pub", subject, msg))

    async def flush(self, timeout=0):
        self.actions.append(("flush", timeout))


def test_base_transport_not_implemented():
    with pytest.raises(NotImplementedError):
        BaseTransport().enqueue("x")


def test_async_base_transport_not_implemented():
    with pytest.raises(NotImplementedError):
        asyncio.run(AsyncBaseTransport().enqueue("x"))


def test_get_client_types():
    assert isinstance(get_client("grpc", stub=DummyStub()), GrpcClient)
    assert isinstance(get_client("nats", connection=DummyConn()), NatsClient)
    assert isinstance(get_client("grpc_async", stub=DummyAsyncStub()), AsyncGrpcClient)
    assert isinstance(get_client("nats_async", connection=DummyAsyncConn()), AsyncNatsClient)
    with pytest.raises(ValueError):
        get_client("unknown")


def test_grpc_and_nats_clients():
    stub = DummyStub()
    conn = DummyConn()
    g = GrpcClient(stub)
    n = NatsClient(conn, subject="demo")
    g.enqueue("msg", timeout=0.1)
    n.enqueue(b"data", timeout=0.2)
    assert stub.called == [("msg", 0.1)]
    assert conn.actions == [("pub", "demo", b"data"), ("flush", 0.2)]


def test_async_grpc_and_nats_clients():
    stub = DummyAsyncStub()
    conn = DummyAsyncConn()
    g = AsyncGrpcClient(stub)
    n = AsyncNatsClient(conn, subject="demo")
    asyncio.run(g.enqueue("msg", timeout=0.1))
    asyncio.run(n.enqueue(b"data", timeout=0.2))
    assert stub.called == [("msg", 0.1)]
    assert conn.actions == [("pub", "demo", b"data"), ("flush", 0.2)]
