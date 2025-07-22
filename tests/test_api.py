from fastapi.testclient import TestClient

import tempfile

from task_cascadence.api import app
from task_cascadence.scheduler import CronScheduler
from task_cascadence.plugins import CronTask


class DummyTask(CronTask):
    name = "dummy"

    def __init__(self):
        self.ran = 0

    def run(self):
        self.ran += 1
        return "ok"


def setup_scheduler(monkeypatch):
    sched = CronScheduler(storage_path="/tmp/sched.yml")
    task = DummyTask()
    sched.register_task(name_or_task="dummy", task_or_expr=task)
    monkeypatch.setattr("task_cascadence.api.get_default_scheduler", lambda: sched)
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda run, user_id=None: None)
    tmp = tempfile.NamedTemporaryFile(delete=False)
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", tmp.name)
    return sched, task


def test_list_tasks(monkeypatch):
    sched, _task = setup_scheduler(monkeypatch)
    client = TestClient(app)
    resp = client.get("/tasks")
    assert resp.status_code == 200
    assert resp.json() == [{"name": "dummy", "disabled": False}]


def test_run_task(monkeypatch):
    sched, task = setup_scheduler(monkeypatch)
    client = TestClient(app)
    resp = client.post("/tasks/dummy/run")
    assert resp.status_code == 200
    assert resp.json() == {"result": "ok"}
    assert task.ran == 1


def test_run_task_user_header(monkeypatch):
    called = {}

    def fake_run(name, use_temporal=False, user_id=None):
        called["uid"] = user_id
        return "r"

    sched, _ = setup_scheduler(monkeypatch)
    monkeypatch.setattr(sched, "run_task", fake_run)
    client = TestClient(app)
    client.post("/tasks/dummy/run", headers={"X-User-ID": "alice"})
    assert called["uid"] == "alice"


def test_schedule_task(monkeypatch):
    sched, _ = setup_scheduler(monkeypatch)
    client = TestClient(app)
    resp = client.post("/tasks/dummy/schedule", params={"expression": "*/5 * * * *"})
    assert resp.status_code == 200
    job = sched.scheduler.get_job("DummyTask")
    assert job is not None


def test_disable_task(monkeypatch):
    sched, _ = setup_scheduler(monkeypatch)
    client = TestClient(app)
    resp = client.post("/tasks/dummy/disable")
    assert resp.status_code == 200
    assert sched._tasks["dummy"]["disabled"] is True
