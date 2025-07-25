import yaml
from task_cascadence.pointer_store import PointerStore
from task_cascadence.stage_store import StageStore
from task_cascadence.ume import _hash_user_id
from tests.utils.threads import run_workers


def test_pointer_store_parallel_writes(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    path = tmp_path / "pointers.yml"
    store = PointerStore(path=path)

    def worker(_):
        store.add_pointer("demo", "alice", "r1")

    run_workers(worker, workers=5, iterations=10)

    data = yaml.safe_load(path.read_text())
    assert data == {"demo": [{"run_id": "r1", "user_hash": _hash_user_id("alice")}]}


def test_stage_store_parallel_writes(tmp_path):
    path = tmp_path / "stages.yml"
    store = StageStore(path=path)

    def worker(i):
        store.add_event("demo", f"stage-{i}", None)

    run_workers(worker, workers=5, iterations=20)

    data = yaml.safe_load(path.read_text())
    assert len(data["demo"]) == 100
