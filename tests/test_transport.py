import time
import pytest

from task_cascadence.transport import GrpcClient, NatsClient
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
