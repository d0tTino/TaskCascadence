import yaml
from pathlib import Path

from task_cascadence.pointer_store import PointerStore
from task_cascadence.ume import _hash_user_id
import multiprocessing


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


def test_add_pointer_deduplicates_after_reload(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    path = tmp_path / "pointers.yml"
    store = PointerStore(path=path)
    store.add_pointer("t", "alice", "r1")

    # reload store from disk and add the same pointer again
    store = PointerStore(path=path)
    store.add_pointer("t", "alice", "r1")

    data = yaml.safe_load(path.read_text())
    assert data["t"] == [{"run_id": "r1", "user_hash": _hash_user_id("alice")}]


def test_apply_update_deduplicates_after_reload(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    path = tmp_path / "pointers.yml"
    store = PointerStore(path=path)
    from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate

    update = PointerUpdate(
        task_name="t", run_id="r1", user_hash=_hash_user_id("alice")
    )
    store.apply_update(update)

    # reload store and apply the same update again
    store = PointerStore(path=path)
    store.apply_update(update)

    data = yaml.safe_load(path.read_text())
    assert data["t"] == [{"run_id": "r1", "user_hash": _hash_user_id("alice")}]


def _proc_add(path: Path, run_id: str) -> None:
    store = PointerStore(path=path)
    store.add_pointer("t", "alice", run_id)


def test_concurrent_add_pointer(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    path = tmp_path / "pointers.yml"

    procs = [multiprocessing.Process(target=_proc_add, args=(path, str(i))) for i in range(5)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

    data = yaml.safe_load(path.read_text())
    run_ids = sorted(e["run_id"] for e in data["t"])
    assert run_ids == [str(i) for i in range(5)]


def _proc_update(path: Path, run_id: str) -> None:
    from task_cascadence.ume.protos.tasks_pb2 import PointerUpdate

    store = PointerStore(path=path)
    store.apply_update(
        PointerUpdate(task_name="t", run_id=run_id, user_hash=_hash_user_id("alice"))
    )


def test_concurrent_apply_update(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    path = tmp_path / "pointers.yml"

    procs = [multiprocessing.Process(target=_proc_update, args=(path, str(i))) for i in range(5)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

    data = yaml.safe_load(path.read_text())
    run_ids = sorted(e["run_id"] for e in data["t"])
    assert run_ids == [str(i) for i in range(5)]


def test_per_user_and_group_isolation(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    store = PointerStore(path=tmp_path / "pointers.yml")

    store.add_pointer("t", "alice", "r1", group_id="g1")
    store.add_pointer("t", "alice", "r2", group_id="g2")
    store.add_pointer("t", "bob", "r3", group_id="g1")

    u_alice = _hash_user_id("alice")
    u_bob = _hash_user_id("bob")

    assert [p["run_id"] for p in store.get_pointers("t", u_alice, "g1")] == ["r1"]
    assert [p["run_id"] for p in store.get_pointers("t", u_alice, "g2")] == ["r2"]
    assert [p["run_id"] for p in store.get_pointers("t", u_bob, "g1")] == ["r3"]
    assert store.get_pointers("t", u_bob, "g2") == []
