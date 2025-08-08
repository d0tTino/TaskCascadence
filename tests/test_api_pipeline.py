from fastapi.testclient import TestClient

from task_cascadence.api import app
from task_cascadence.scheduler import CronScheduler
from task_cascadence.plugins import ExampleTask
from task_cascadence.stage_store import StageStore
from task_cascadence.pipeline_registry import get_pipeline
import threading
import time


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

    resp = client.post("/tasks/example/pause", headers={"X-User-ID": "alice"})
    assert resp.status_code == 200
    assert sched._tasks["example"]["paused"] is True
    events = StageStore(path=tmp_path / "stages.yml").get_events("example")
    assert events[-1]["stage"] == "paused"
    assert "time" in events[-1]

    resp = client.post("/tasks/example/resume", headers={"X-User-ID": "alice"})
    assert resp.status_code == 200
    assert sched._tasks["example"]["paused"] is False
    events = StageStore(path=tmp_path / "stages.yml").get_events("example")
    assert events[-1]["stage"] == "resumed"
    assert "time" in events[-1]


class SlowTask(ExampleTask):
    name = "slow"

    def intake(self):
        time.sleep(0.05)

    def run(self):
        time.sleep(0.2)
        return "ok"


def test_api_pause_running_pipeline(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "s2.yml"))
    import task_cascadence.ume as ume
    ume._stage_store = None

    sched = CronScheduler(storage_path=tmp_path / "sched2.yml")
    task = SlowTask()
    sched.register_task("slow", task)
    monkeypatch.setattr("task_cascadence.api.get_default_scheduler", lambda: sched)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda *a, **k: None)

    client = TestClient(app)
    thread = threading.Thread(target=lambda: sched.run_task("slow", user_id="bob"))
    thread.start()
    time.sleep(0.05)
    resp = client.post("/tasks/slow/pause", headers={"X-User-ID": "bob"})
    assert resp.status_code == 200
    pipeline = get_pipeline("slow")
    assert pipeline is not None and pipeline._paused is True
    assert thread.is_alive()
    resp = client.post("/tasks/slow/resume", headers={"X-User-ID": "bob"})
    assert resp.status_code == 200
    thread.join()
    assert pipeline._paused is False


def test_api_pipeline_status(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "stages.yml"))
    StageStore().add_event("example", "start", None)
    StageStore().add_event("example", "finish", None)
    events = StageStore().get_events("example")

    setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)

    resp = client.get("/pipeline/example")
    assert resp.status_code == 200
    assert resp.json() == events
