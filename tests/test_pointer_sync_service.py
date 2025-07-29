import yaml
import importlib
from typer.testing import CliRunner

from task_cascadence import pointer_sync
from task_cascadence.cli import app, pointer_sync_cmd
import asyncio


def test_pointer_sync_grpc(monkeypatch, tmp_path):
    store = tmp_path / "pointers.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store))
    monkeypatch.setenv("UME_TRANSPORT", "grpc")
    monkeypatch.setenv("UME_GRPC_METHOD", "Subscribe")

    module = tmp_path / "stub.py"
    module.write_text(
        """
class Stub:
    @staticmethod
    def Subscribe():
        from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate
        return [PointerUpdate(task_name='demo', run_id='r1', user_hash='u')]
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("UME_GRPC_STUB", "stub:Stub")

    importlib.invalidate_caches()
    pointer_sync.run()

    data = yaml.safe_load(store.read_text())
    assert data["demo"] == [{"run_id": "r1", "user_hash": "u"}]


def test_pointer_sync_nats(monkeypatch, tmp_path):
    store = tmp_path / "pointers.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store))
    monkeypatch.setenv("UME_TRANSPORT", "nats")

    module = tmp_path / "conn.py"
    module.write_text(
        """
class Conn:
    def subscribe_sync(self, subject):
        from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate
        update = PointerUpdate(task_name='demo', run_id='r2', user_hash='x')
        return [update.SerializeToString()]
conn = Conn()
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("UME_NATS_CONN", "conn:conn")
    monkeypatch.setenv("UME_NATS_SUBJECT", "events")

    importlib.invalidate_caches()
    pointer_sync.run()

    data = yaml.safe_load(store.read_text())
    assert data["demo"] == [{"run_id": "r2", "user_hash": "x"}]


def test_cli_pointer_sync_grpc(monkeypatch, tmp_path):
    store = tmp_path / "pointers.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store))
    monkeypatch.setenv("UME_TRANSPORT", "grpc")
    monkeypatch.setenv("UME_GRPC_METHOD", "Subscribe")

    module = tmp_path / "stub_cli.py"
    module.write_text(
        """
class Stub:
    @staticmethod
    def Subscribe():
        from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate
        return [PointerUpdate(task_name='demo', run_id='cli1', user_hash='u')]
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("UME_GRPC_STUB", "stub_cli:Stub")

    importlib.invalidate_caches()
    runner = CliRunner()
    result = runner.invoke(app, ["pointer-sync"])
    assert result.exit_code == 0

    data = yaml.safe_load(store.read_text())
    assert data["demo"] == [{"run_id": "cli1", "user_hash": "u"}]


def test_cli_pointer_sync_nats(monkeypatch, tmp_path):
    store = tmp_path / "pointers.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store))
    monkeypatch.setenv("UME_TRANSPORT", "nats")

    module = tmp_path / "conn_cli.py"
    module.write_text(
        """
class Conn:
    def subscribe_sync(self, subject):
        from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate
        update = PointerUpdate(task_name='demo', run_id='cli2', user_hash='x')
        return [update.SerializeToString()]
conn = Conn()
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("UME_NATS_CONN", "conn_cli:conn")
    monkeypatch.setenv("UME_NATS_SUBJECT", "events")

    importlib.invalidate_caches()
    runner = CliRunner()
    result = runner.invoke(app, ["pointer-sync"])
    assert result.exit_code == 0

    data = yaml.safe_load(store.read_text())
    assert data["demo"] == [{"run_id": "cli2", "user_hash": "x"}]


def test_pointer_sync_broadcast(monkeypatch, tmp_path):
    store_a = tmp_path / "a.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store_a))
    monkeypatch.setenv("UME_TRANSPORT", "grpc")
    monkeypatch.setenv("UME_GRPC_METHOD", "Subscribe_initial")
    monkeypatch.setenv("UME_BROADCAST_POINTERS", "1")

    module = tmp_path / "stub_b.py"
    module.write_text(
        """
from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate
messages = []

class Stub:
    @staticmethod
    def Subscribe_initial():
        return [PointerUpdate(task_name='demo', run_id='r1', user_hash='u')]

    @staticmethod
    def Subscribe_queue():
        out = list(messages)
        messages.clear()
        return out

    @staticmethod
    def Send(update, timeout=None):
        messages.append(update)
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("UME_GRPC_STUB", "stub_b:Stub")

    importlib.invalidate_caches()
    monkeypatch.setattr(pointer_sync, "emit_pointer_update", __import__("stub_b").Stub.Send)
    pointer_sync.run()

    data = yaml.safe_load(store_a.read_text())
    assert data["demo"] == [{"run_id": "r1", "user_hash": "u"}]

    store_b = tmp_path / "b.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store_b))
    monkeypatch.setenv("UME_BROADCAST_POINTERS", "0")
    monkeypatch.setenv("UME_GRPC_METHOD", "Subscribe_queue")

    pointer_sync.run()

    data = yaml.safe_load(store_b.read_text())
    assert data["demo"] == [{"run_id": "r1", "user_hash": "u"}]


def test_pointer_sync_async_grpc(monkeypatch, tmp_path):
    store = tmp_path / "pointers.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store))
    monkeypatch.setenv("UME_TRANSPORT", "grpc")
    monkeypatch.setenv("UME_GRPC_METHOD", "Subscribe")

    module = tmp_path / "astub.py"
    module.write_text(
        """
import asyncio
from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate
class Stub:
    @staticmethod
    async def Subscribe():
        yield PointerUpdate(task_name='demo', run_id='ra1', user_hash='u')
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("UME_GRPC_STUB", "astub:Stub")

    importlib.invalidate_caches()
    asyncio.run(pointer_sync.run_async())

    data = yaml.safe_load(store.read_text())
    assert data["demo"] == [{"run_id": "ra1", "user_hash": "u"}]


def test_pointer_sync_async_nats(monkeypatch, tmp_path):
    store = tmp_path / "pointers.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store))
    monkeypatch.setenv("UME_TRANSPORT", "nats")

    module = tmp_path / "aconn.py"
    module.write_text(
        """
import asyncio
from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate
class Conn:
    async def subscribe(self, subject):
        update = PointerUpdate(task_name='demo', run_id='ra2', user_hash='x')
        yield update.SerializeToString()
conn = Conn()
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("UME_NATS_CONN", "aconn:conn")
    monkeypatch.setenv("UME_NATS_SUBJECT", "events")

    importlib.invalidate_caches()
    asyncio.run(pointer_sync.run_async())

    data = yaml.safe_load(store.read_text())
    assert data["demo"] == [{"run_id": "ra2", "user_hash": "x"}]


def test_cli_pointer_sync_async(monkeypatch):
    called = {}

    async def fake_run_async():
        called["async"] = True

    def fake_run():
        called["sync"] = True

    monkeypatch.setattr(pointer_sync, "run_async", fake_run_async)
    monkeypatch.setattr(pointer_sync, "run", fake_run)

    async def runner():
        pointer_sync_cmd()
        await asyncio.sleep(0)

    asyncio.run(runner())

    assert called == {"async": True}


def test_pointer_sync_async_grpc_broadcast(monkeypatch, tmp_path):
    store = tmp_path / "pointers.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store))
    monkeypatch.setenv("UME_TRANSPORT", "grpc")
    monkeypatch.setenv("UME_GRPC_METHOD", "Subscribe")
    monkeypatch.setenv("UME_BROADCAST_POINTERS", "1")

    module = tmp_path / "bcast_stub.py"
    module.write_text(
        """
import asyncio
from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate
class Stub:
    @staticmethod
    async def Subscribe():
        yield PointerUpdate(task_name='demo', run_id='rab1', user_hash='u')
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("UME_GRPC_STUB", "bcast_stub:Stub")

    captured = {}

    def fake_emit(update, **_: str):
        captured["update"] = update

    monkeypatch.setattr(pointer_sync, "emit_pointer_update", fake_emit)

    importlib.invalidate_caches()
    asyncio.run(pointer_sync.run_async())

    data = yaml.safe_load(store.read_text())
    assert data["demo"] == [{"run_id": "rab1", "user_hash": "u"}]
    assert captured["update"].run_id == "rab1"


def test_pointer_sync_async_grpc_no_broadcast(monkeypatch, tmp_path):
    store = tmp_path / "pointers.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store))
    monkeypatch.setenv("UME_TRANSPORT", "grpc")
    monkeypatch.setenv("UME_GRPC_METHOD", "Subscribe")
    monkeypatch.setenv("UME_BROADCAST_POINTERS", "0")

    module = tmp_path / "nobcast_stub.py"
    module.write_text(
        """
import asyncio
from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate
class Stub:
    @staticmethod
    async def Subscribe():
        yield PointerUpdate(task_name='demo', run_id='rab2', user_hash='u')
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("UME_GRPC_STUB", "nobcast_stub:Stub")

    captured = {}

    def fake_emit(update, **_: str):
        captured["update"] = update

    monkeypatch.setattr(pointer_sync, "emit_pointer_update", fake_emit)

    importlib.invalidate_caches()
    asyncio.run(pointer_sync.run_async())

    data = yaml.safe_load(store.read_text())
    assert data["demo"] == [{"run_id": "rab2", "user_hash": "u"}]
    assert captured == {}


