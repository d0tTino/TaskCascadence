import yaml
from pathlib import Path

from task_cascadence.pointer_store import PointerStore
from task_cascadence.ume import _hash_user_id


def test_add_pointer_writes_hashed(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    default = Path.home() / ".cascadence" / "pointers.yml"
    if default.exists():
        default.unlink()
    path = tmp_path / "pointers.yml"
    store = PointerStore(path=path)
    store.add_pointer("t", "alice", "r1")

    data = yaml.safe_load(path.read_text())
    assert data["t"][0]["run_id"] == "r1"
    assert data["t"][0]["user_hash"] == _hash_user_id("alice")
    assert data["t"][0]["user_hash"] != "alice"


def test_get_pointers_persist(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    default = Path.home() / ".cascadence" / "pointers.yml"
    if default.exists():
        default.unlink()
    path = tmp_path / "pointers.yml"
    store1 = PointerStore(path=path)
    store1.add_pointer("t", "alice", "r1")

    store2 = PointerStore(path=path)
    assert store2.get_pointers("t") == [
        {"run_id": "r1", "user_hash": _hash_user_id("alice")}
    ]


def test_add_pointer_deduplicates(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    path = tmp_path / "pointers.yml"
    store = PointerStore(path=path)

    store.add_pointer("t", "alice", "r1")
    store.add_pointer("t", "alice", "r1")

    data = yaml.safe_load(path.read_text())
    assert data["t"] == [{"run_id": "r1", "user_hash": _hash_user_id("alice")}] 


def test_apply_update_deduplicates(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    path = tmp_path / "pointers.yml"
    store = PointerStore(path=path)
    from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate

    update = PointerUpdate(
        task_name="t", run_id="r1", user_hash=_hash_user_id("alice")
    )
    store.apply_update(update)
    store.apply_update(update)

    data = yaml.safe_load(path.read_text())
    assert data["t"] == [{"run_id": "r1", "user_hash": _hash_user_id("alice")}] 

