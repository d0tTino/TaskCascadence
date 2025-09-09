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

    def fake_emit(update, **_):
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
    monkeypatch.setattr(
        pointer_sync,
        "emit_pointer_update",
        lambda update, **_: stub_module.Stub.Send(update),
    )

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


def test_pointer_sync_context_propagation(monkeypatch, tmp_path):
    store = tmp_path / "pointers.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store))
    monkeypatch.setenv("UME_TRANSPORT", "grpc")
    monkeypatch.setenv("UME_GRPC_METHOD", "Subscribe")
    monkeypatch.setenv("UME_BROADCAST_POINTERS", "1")

    module = tmp_path / "ctx_stub.py"
    module.write_text(
        """
from task_cascadence.ume import _hash_user_id
class Stub:
    @staticmethod
    def Subscribe():
        from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate
        return [
            PointerUpdate(
                task_name='demo',
                run_id='r1',
                user_id='u1',
                user_hash=_hash_user_id('u1'),
                group_id='g1',
            )
        ]
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("UME_GRPC_STUB", "ctx_stub:Stub")

    import importlib

    importlib.invalidate_caches()

    captured_emit: dict[str, dict[str, str | None]] = {}

    def fake_emit(update, **kwargs):
        captured_emit["kwargs"] = kwargs

    audits: list[tuple[str, str, str, str | None, str | None, str | None]] = []

    def fake_audit(
        task, stage, status, *, reason=None, user_id=None, group_id=None, **_
    ):
        audits.append((task, stage, status, reason, user_id, group_id))

    monkeypatch.setattr(pointer_sync, "emit_pointer_update", fake_emit)
    monkeypatch.setattr(pointer_sync, "emit_audit_log", fake_audit)

    pointer_sync.run()

    assert captured_emit["kwargs"]["user_id"] == "u1"
    assert captured_emit["kwargs"]["group_id"] == "g1"
    assert ("demo", "pointer_sync", "success", None, "u1", "g1") in audits
    data = yaml.safe_load(store.read_text())
    assert data["demo"][0]["group_id"] == "g1"


def test_pointer_sync_audit_failure(monkeypatch, tmp_path):
    store = tmp_path / "pointers.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store))
    monkeypatch.setenv("UME_TRANSPORT", "grpc")
    monkeypatch.setenv("UME_GRPC_METHOD", "Subscribe")

    module = tmp_path / "fail_stub.py"
    module.write_text(
        """
from task_cascadence.ume import _hash_user_id
class Stub:
    @staticmethod
    def Subscribe():
        from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate
        return [
            PointerUpdate(
                task_name='demo',
                run_id='r2',
                user_id='u2',
                user_hash=_hash_user_id('u2'),
            )
        ]
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("UME_GRPC_STUB", "fail_stub:Stub")

    import importlib

    importlib.invalidate_caches()

    def boom(self, update, group_id=None):  # type: ignore[override]
        raise RuntimeError("boom")

    monkeypatch.setattr(pointer_sync.PointerStore, "apply_update", boom)

    audits: list[tuple[str, str, str, str | None, str | None, str | None]] = []

    def fake_audit(
        task, stage, status, *, reason=None, user_id=None, group_id=None, **_
    ):
        audits.append((task, stage, status, reason, user_id, group_id))

    monkeypatch.setattr(pointer_sync, "emit_audit_log", fake_audit)

    pointer_sync.run()

    assert ("demo", "pointer_sync", "error", "boom", "u2", None) in audits
