import threading

import yaml
from task_cascadence.stage_store import StageStore


def test_concurrent_writes(tmp_path):
    path = tmp_path / "stages.yml"
    barrier = threading.Barrier(5)
    errors = []

    store = StageStore(path=path)

    def worker(i: int) -> None:
        try:
            barrier.wait()
            store.add_event("t", f"s{i}", None)
        except Exception as e:  # pragma: no cover - failure info
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    data = yaml.safe_load(path.read_text())
    stages = sorted(e["stage"] for e in data["t"])
    assert stages == [f"s{i}" for i in range(5)]


def test_add_event_threads(tmp_path):
    path = tmp_path / "stages.yml"
    store = StageStore(path=path)

    def worker(_: int) -> None:
        store.add_event("t", "s", None)

    from tests.utils.threads import run_workers

    run_workers(worker, workers=2, iterations=3)

    data = yaml.safe_load(path.read_text())
    assert len(data["t"]) == 6


def test_per_user_and_group_isolation(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    from task_cascadence.ume import _hash_user_id

    store = StageStore(path=tmp_path / "stages.yml")
    u1 = _hash_user_id("alice")
    u2 = _hash_user_id("bob")

    store.add_event("t", "s1", u1, "g1")
    store.add_event("t", "s2", u1, "g2")
    store.add_event("t", "s3", u2, "g1")

    assert [e["stage"] for e in store.get_events("t", u1, "g1")] == ["s1"]
    assert [e["stage"] for e in store.get_events("t", u1, "g2")] == ["s2"]
    assert [e["stage"] for e in store.get_events("t", u2, "g1")] == ["s3"]
    assert store.get_events("t", u2, "g2") == []


def test_run_id_is_recorded(tmp_path):
    store = StageStore(path=tmp_path / "stages.yml")
    store.add_event("demo", "run", None, run_id="run-123")

    events = store.get_events("demo")
    assert events[0]["run_id"] == "run-123"
