from fastapi.testclient import TestClient

from task_cascadence.dashboard import app, StageStore
from task_cascadence.scheduler import BaseScheduler
from task_cascadence.plugins import ExampleTask, PointerTask


def setup(monkeypatch, tmp_path):
    sched = BaseScheduler()
    task = ExampleTask()
    sched.register_task("example", task)
    monkeypatch.setattr(
        "task_cascadence.dashboard.get_default_scheduler", lambda: sched
    )
    store = StageStore(path=tmp_path / "stages.yml")
    monkeypatch.setattr("task_cascadence.dashboard.StageStore", lambda: store)
    return sched, store


def test_dashboard_index(monkeypatch, tmp_path):
    sched, store = setup(monkeypatch, tmp_path)
    store.add_event("example", "run", None)
    ts = store.get_events("example")[0]["time"]
    client = TestClient(app)
    resp = client.get("/")
    assert resp.status_code == 200
    assert "example" in resp.text
    assert "run" in resp.text
    assert ts in resp.text


def test_pause_resume(monkeypatch, tmp_path):
    sched, _ = setup(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post("/pause/example", follow_redirects=False)
    assert resp.status_code == 303
    assert sched._tasks["example"]["paused"] is True
    resp = client.post("/resume/example", follow_redirects=False)
    assert resp.status_code == 303
    assert sched._tasks["example"]["paused"] is False


def test_dashboard_pointer_counts(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(tmp_path / "pointers.yml"))

    class DemoPointer(PointerTask):
        name = "demo_pointer"

    sched = BaseScheduler()
    task = DemoPointer()
    task.add_pointer("alice", "run1")
    task.add_pointer("bob", "run2")
    sched.register_task("demo_pointer", task)
    monkeypatch.setattr(
        "task_cascadence.dashboard.get_default_scheduler",
        lambda: sched,
    )
    store = StageStore(path=tmp_path / "stages.yml")
    monkeypatch.setattr("task_cascadence.dashboard.StageStore", lambda: store)

    client = TestClient(app)
    resp = client.get("/")

    assert resp.status_code == 200
    assert "<th>Pointers</th>" in resp.text
    assert "<td>2</td>" in resp.text
