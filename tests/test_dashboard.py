from fastapi.testclient import TestClient

from task_cascadence.dashboard import app, StageStore, PointerStore
from task_cascadence.scheduler import BaseScheduler
from task_cascadence.plugins import ExampleTask


def setup(monkeypatch, tmp_path):
    sched = BaseScheduler()
    task = ExampleTask()
    sched.register_task("example", task)
    monkeypatch.setattr(
        "task_cascadence.dashboard.get_default_scheduler", lambda: sched
    )
    stage_store = StageStore(path=tmp_path / "stages.yml")
    pointer_store = PointerStore(path=tmp_path / "pointers.yml")
    monkeypatch.setattr("task_cascadence.dashboard.StageStore", lambda: stage_store)
    monkeypatch.setattr("task_cascadence.dashboard.PointerStore", lambda: pointer_store)
    return sched, stage_store, pointer_store


def test_dashboard_index(monkeypatch, tmp_path):
    sched, s_store, p_store = setup(monkeypatch, tmp_path)
    s_store.add_event("example", "run", None)
    p_store.add_pointer("example", "alice", "r1")
    ts = s_store.get_events("example")[0]["time"]
    client = TestClient(app)
    resp = client.get("/")
    assert resp.status_code == 200
    assert "example" in resp.text
    assert "run" in resp.text
    assert ts in resp.text
    assert "<td>1</td>" in resp.text


def test_pause_resume(monkeypatch, tmp_path):
    sched, _, _ = setup(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post("/pause/example", follow_redirects=False)
    assert resp.status_code == 303
    assert sched._tasks["example"]["paused"] is True
    resp = client.post("/resume/example", follow_redirects=False)
    assert resp.status_code == 303
    assert sched._tasks["example"]["paused"] is False
