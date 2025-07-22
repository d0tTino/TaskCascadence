import time
import asyncio
import pytest

from task_cascadence.transport import (
    GrpcClient,
    NatsClient,
    AsyncGrpcClient,
    AsyncNatsClient,
)
from task_cascadence.ume import emit_task_spec
from task_cascadence.ume.models import TaskSpec


class Stub:
    def __init__(self):
        self.timeout = None
        self.received = None

    def Send(self, msg, timeout=None):
        self.timeout = timeout
        self.received = msg


class SlowStub:
    def Send(self, msg, timeout=None):
        time.sleep(0.3)


class AsyncStub:
    def __init__(self):
        self.timeout = None
        self.received = None

    async def Send(self, msg, timeout=None):
        self.timeout = timeout
        self.received = msg


class AsyncSlowStub:
    async def Send(self, msg, timeout=None):
        await asyncio.sleep(0.3)


class FakeConn:
    def __init__(self):
        self.published = []
        self.flushed = None

    def publish(self, subject, msg):
        self.published.append((subject, msg))

    def flush(self, timeout=0):
        self.flushed = timeout


class SlowConn(FakeConn):
    def flush(self, timeout=0):
        time.sleep(0.3)
        super().flush(timeout)


class AsyncConn:
    def __init__(self):
        self.published = []
        self.flushed = None

    async def publish(self, subject, msg):
        self.published.append((subject, msg))

    async def flush(self, timeout=0):
        self.flushed = timeout


class AsyncSlowConn(AsyncConn):
    async def flush(self, timeout=0):
        await asyncio.sleep(0.3)
        await super().flush(timeout)


def test_grpc_client_passes_timeout():
    stub = Stub()
    client = GrpcClient(stub)
    client.enqueue("data", timeout=0.1)
    assert stub.timeout == 0.1
    assert stub.received == "data"


def test_grpc_deadline_exceeded():
    stub = SlowStub()
    client = GrpcClient(stub)
    spec = TaskSpec(id="1", name="demo")
    with pytest.raises(RuntimeError):
        emit_task_spec(spec, client=client)


def test_nats_client_passes_timeout():
    conn = FakeConn()
    client = NatsClient(conn, subject="demo")
    client.enqueue("msg", timeout=0.15)
    assert conn.flushed == 0.15
    assert conn.published == [("demo", "msg")]


def test_nats_deadline_exceeded():
    conn = SlowConn()
    client = NatsClient(conn)
    spec = TaskSpec(id="2", name="nats")
    with pytest.raises(RuntimeError):
        emit_task_spec(spec, client=client)


def test_async_grpc_client_passes_timeout():
    stub = AsyncStub()
    client = AsyncGrpcClient(stub)

    async def run():
        await client.enqueue("data", timeout=0.1)

    asyncio.run(run())
    assert stub.timeout == 0.1
    assert stub.received == "data"


def test_async_grpc_deadline_exceeded():
    stub = AsyncSlowStub()
    client = AsyncGrpcClient(stub)
    spec = TaskSpec(id="3", name="ademo")

    async def run():
        task = emit_task_spec(spec, client=client, use_asyncio=True)
        await task

    with pytest.raises(RuntimeError):
        asyncio.run(run())


def test_async_nats_client_passes_timeout():
    conn = AsyncConn()
    client = AsyncNatsClient(conn, subject="demo")

    async def run():
        await client.enqueue("msg", timeout=0.15)

    asyncio.run(run())
    assert conn.flushed == 0.15
    assert conn.published == [("demo", "msg")]


def test_async_nats_deadline_exceeded():
    conn = AsyncSlowConn()
    client = AsyncNatsClient(conn)
    spec = TaskSpec(id="4", name="async-nats")

    async def run():
        task = emit_task_spec(spec, client=client, use_asyncio=True)
        await task

    with pytest.raises(RuntimeError):
        asyncio.run(run())
