from fastapi.testclient import TestClient

from task_cascadence.api import app
from task_cascadence.scheduler import CronScheduler
from task_cascadence.plugins import ExampleTask
from task_cascadence.stage_store import StageStore


def setup_scheduler(monkeypatch, tmp_path):
    sched = CronScheduler(storage_path=tmp_path / "sched.yml")
    task = ExampleTask()
    sched.register_task("example", task)
    monkeypatch.setattr("task_cascadence.api.get_default_scheduler", lambda: sched)
    return sched


def test_api_pause_resume(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "stages.yml"))
    import task_cascadence.ume as ume
    ume._stage_store = None

    sched = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)

    resp = client.post("/tasks/example/pause")
    assert resp.status_code == 200
    assert sched._tasks["example"]["paused"] is True
    events = StageStore(path=tmp_path / "stages.yml").get_events("example")
    assert events[-1]["stage"] == "paused"

    resp = client.post("/tasks/example/resume")
    assert resp.status_code == 200
    assert sched._tasks["example"]["paused"] is False
    events = StageStore(path=tmp_path / "stages.yml").get_events("example")
    assert events[-1]["stage"] == "resumed"


def test_api_pipeline_status(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "stages.yml"))
    StageStore().add_event("example", "start", None)
    StageStore().add_event("example", "finish", None)

    setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)

    resp = client.get("/pipeline/example")
    assert resp.status_code == 200
    assert resp.json() == [{"stage": "start"}, {"stage": "finish"}]
