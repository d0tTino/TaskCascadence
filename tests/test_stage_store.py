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
