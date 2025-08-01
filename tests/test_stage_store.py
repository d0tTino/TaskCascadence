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
