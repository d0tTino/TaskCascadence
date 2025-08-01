import yaml
from typer.testing import CliRunner

import pytest
import asyncio

from task_cascadence import pointer_sync
from task_cascadence.pointer_store import PointerStore
from task_cascadence.cli import app
from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate


def test_pointer_store_emits(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(tmp_path / "pointers.yml"))
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    captured = {}

    def fake_emit(update):
        captured["update"] = update

    monkeypatch.setattr(
        "task_cascadence.pointer_store.emit_pointer_update", fake_emit
    )

    store = PointerStore()
    store.add_pointer("demo", "alice", "run1")

    assert isinstance(captured["update"], PointerUpdate)
    assert captured["update"].task_name == "demo"
    assert captured["update"].run_id == "run1"
    assert captured["update"].user_hash != "alice"


def test_cli_send_receive(monkeypatch, tmp_path):
    store = tmp_path / "pointers.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store))
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    emitted = {}

    def fake_emit(update):
        emitted["update"] = update

    monkeypatch.setattr("task_cascadence.ume.emit_pointer_update", fake_emit)

    runner = CliRunner()
    result = runner.invoke(app, ["pointer-send", "demo", "alice", "run1"])
    assert result.exit_code == 0
    user_hash = emitted["update"].user_hash
    assert user_hash != "alice"

    result = runner.invoke(app, ["pointer-receive", "demo", "run1", user_hash])
    assert result.exit_code == 0

    data = yaml.safe_load(store.read_text())
    assert data["demo"][0]["run_id"] == "run1"
    assert data["demo"][0]["user_hash"] == user_hash


def test_pointer_sync_broadcast(monkeypatch, tmp_path):
    store = tmp_path / "pointers.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store))
    monkeypatch.setenv("UME_TRANSPORT", "grpc")
    monkeypatch.setenv("UME_GRPC_METHOD", "Subscribe")
    monkeypatch.setenv("UME_BROADCAST_POINTERS", "1")

    module = tmp_path / "stub_broadcast.py"
    module.write_text(
        """
class Stub:
    @staticmethod
    def Subscribe():
        from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate
        return [PointerUpdate(task_name='demo', run_id='b1', user_hash='u')]
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("UME_GRPC_STUB", "stub_broadcast:Stub")

    import importlib

    importlib.invalidate_caches()

    captured = {}

    def fake_emit(update):
        captured["update"] = update

    monkeypatch.setattr(pointer_sync, "emit_pointer_update", fake_emit)

    pointer_sync.run()

    assert captured["update"].task_name == "demo"

    data = yaml.safe_load(store.read_text())
    assert data["demo"] == [{"run_id": "b1", "user_hash": "u"}]


def test_pointer_sync_incomplete_config(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(tmp_path / "pointers.yml"))
    monkeypatch.setenv("UME_TRANSPORT", "grpc")
    monkeypatch.delenv("UME_GRPC_STUB", raising=False)

    with pytest.raises(ValueError):
        pointer_sync.run()


def test_pointer_sync_async_two_listeners(monkeypatch, tmp_path):
    store_a = tmp_path / "a.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store_a))
    monkeypatch.setenv("UME_TRANSPORT", "grpc")
    monkeypatch.setenv("UME_GRPC_METHOD", "Subscribe_initial")
    monkeypatch.setenv("UME_BROADCAST_POINTERS", "1")

    module = tmp_path / "astub_broadcast.py"
    module.write_text(
        """
import asyncio
from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate
messages = []

class Stub:
    @staticmethod
    async def Subscribe_initial():
        yield PointerUpdate(task_name='demo', run_id='ab1', user_hash='u')

    @staticmethod
    async def Subscribe_queue():
        while messages:
            yield messages.pop(0)
        await asyncio.sleep(0)

    @staticmethod
    def Send(update, timeout=None):
        messages.append(update)
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("UME_GRPC_STUB", "astub_broadcast:Stub")

    import importlib

    importlib.invalidate_caches()
    stub_module = __import__("astub_broadcast")
    monkeypatch.setattr(pointer_sync, "emit_pointer_update", stub_module.Stub.Send)

    asyncio.run(pointer_sync.run_async())

    data = yaml.safe_load(store_a.read_text())
    assert data["demo"] == [{"run_id": "ab1", "user_hash": "u"}]

    store_b = tmp_path / "b.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store_b))
    monkeypatch.setenv("UME_GRPC_METHOD", "Subscribe_queue")
    monkeypatch.setenv("UME_BROADCAST_POINTERS", "0")

    asyncio.run(pointer_sync.run_async())

    data = yaml.safe_load(store_b.read_text())
    assert data["demo"] == [{"run_id": "ab1", "user_hash": "u"}]
