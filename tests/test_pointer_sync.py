import yaml
from typer.testing import CliRunner

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
