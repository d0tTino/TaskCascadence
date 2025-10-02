from typing import Any

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

    resp = client.post(
        "/tasks/example/pause",
        headers={"X-User-ID": "alice", "X-Group-ID": "team"},
    )
    assert resp.status_code == 200
    assert sched._tasks["example"]["paused"] is True
    events = StageStore(path=tmp_path / "stages.yml").get_events("example")
    assert events[-1]["stage"] == "paused"
    assert "time" in events[-1]

    resp = client.post(
        "/tasks/example/resume",
        headers={"X-User-ID": "alice", "X-Group-ID": "team"},
    )
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
    resp = client.post(
        "/tasks/slow/pause",
        headers={"X-User-ID": "bob", "X-Group-ID": "builders"},
    )
    assert resp.status_code == 200
    pipeline = get_pipeline("slow")
    assert pipeline is not None and pipeline._paused is True
    assert thread.is_alive()
    resp = client.post(
        "/tasks/slow/resume",
        headers={"X-User-ID": "bob", "X-Group-ID": "builders"},
    )
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


class ContextSignalTask(ExampleTask):
    name = "contextsignal"

    def __init__(self):
        super().__init__()
        self.research_started = threading.Event()
        self.allow_plan = threading.Event()
        self.contexts: list[dict[str, Any]] = []
        self.plan_values: list[dict[str, Any]] = []
        self.run_values: list[dict[str, Any]] = []
        self.verify_values: list[dict[str, Any]] = []

    def research(self):
        self.research_started.set()
        if not self.allow_plan.wait(timeout=1):
            raise AssertionError("plan stage timed out")
        return None

    def plan(self):
        assert self.contexts, "context not delivered"
        value = self.contexts[-1]
        self.plan_values.append(value)
        return value

    def run(self, plan_result):
        self.run_values.append(plan_result)
        return plan_result

    def verify(self, result):
        self.verify_values.append(result)
        return result

    def attach_context(self, payload):
        self.contexts.append(payload)


def test_api_context_signal(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "sig_stages.yml"))
    import task_cascadence.ume as ume

    ume._stage_store = None

    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_stage_update_event",
        lambda *a, **k: None,
    )

    sched = CronScheduler(storage_path=tmp_path / "sig_sched.yml")
    task = ContextSignalTask()
    sched.register_task("contextsignal", task)
    monkeypatch.setattr("task_cascadence.api.get_default_scheduler", lambda: sched)

    client = TestClient(app)

    thread = threading.Thread(
        target=lambda: sched.run_task("contextsignal", user_id="alice")
    )
    thread.start()

    assert task.research_started.wait(timeout=1)

    resp = client.post(
        "/tasks/contextsignal/signal",
        headers={"X-User-ID": "alice", "X-Group-ID": "team"},
        json={"kind": "context", "value": {"note": "hi"}},
    )
    assert resp.status_code == 202
    assert resp.json()["status"] == "accepted"

    task.allow_plan.set()
    thread.join(timeout=2)
    assert not thread.is_alive()

    assert task.contexts == [{"note": "hi"}]
    assert task.plan_values == [{"note": "hi"}]
    assert task.run_values == [{"note": "hi"}]
    assert task.verify_values == [{"note": "hi"}]

    store = StageStore(path=tmp_path / "sig_stages.yml")
    audit_events = store.get_events(task.__class__.__name__, category="audit")
    assert any(
        event.get("stage") == "context" and event.get("status") == "received"
        for event in audit_events
    )
    assert any(
        event.get("stage") == "plan.context"
        and event.get("status") == "consumed"
        for event in audit_events
    )
