from fastapi.testclient import TestClient

import tempfile

from task_cascadence.api import app
from task_cascadence.scheduler import CronScheduler
from task_cascadence.plugins import CronTask
import yaml


class DummyTask(CronTask):
    name = "dummy"

    def __init__(self):
        self.ran = 0

    def run(self):
        self.ran += 1
        return "ok"


class DynamicTask(CronTask):
    name = "dynamic"

    def run(self):
        return "dyn"


class AsyncTask(CronTask):
    name = "async"

    async def run(self):
        return "async"


def setup_scheduler(monkeypatch, tmp_path):
    sched = CronScheduler(storage_path=tmp_path / "sched.yml")
    task = DummyTask()
    sched.register_task(name_or_task="dummy", task_or_expr=task)
    monkeypatch.setattr("task_cascadence.api.get_default_scheduler", lambda: sched)
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda run, user_id=None: None)
    tmp = tempfile.NamedTemporaryFile(delete=False)
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", tmp.name)
    monkeypatch.setenv("CASCADENCE_TASKS_PATH", str(tmp_path / "tasks.yml"))
    return sched, task


def test_list_tasks(monkeypatch, tmp_path):
    sched, _task = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.get("/tasks")
    assert resp.status_code == 200
    assert resp.json() == [{"name": "dummy", "disabled": False}]


def test_run_task(monkeypatch, tmp_path):
    sched, task = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post("/tasks/dummy/run")
    assert resp.status_code == 200
    assert resp.json() == {"result": "ok"}
    assert task.ran == 1


def test_run_task_async_endpoint(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    async_task = AsyncTask()
    sched.register_task(name_or_task="async", task_or_expr=async_task)
    client = TestClient(app)
    resp = client.post("/tasks/async/run-async")
    assert resp.status_code == 200
    assert resp.json() == {"result": "async"}


def test_run_task_user_header(monkeypatch, tmp_path):
    called = {}

    def fake_run(name, use_temporal=False, user_id=None):
        called["uid"] = user_id
        return "r"

    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    monkeypatch.setattr(sched, "run_task", fake_run)
    client = TestClient(app)
    client.post("/tasks/dummy/run", headers={"X-User-ID": "alice"})
    assert called["uid"] == "alice"


def test_schedule_task(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post("/tasks/dummy/schedule", params={"expression": "*/5 * * * *"})
    assert resp.status_code == 200
    job = sched.scheduler.get_job("DummyTask")
    assert job is not None


def test_disable_task(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post("/tasks/dummy/disable")
    assert resp.status_code == 200
    assert sched._tasks["dummy"]["disabled"] is True


def test_register_task(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post("/tasks", params={"path": "tests.test_api:DynamicTask"})
    assert resp.status_code == 200
    assert "dynamic" in [name for name, _ in sched.list_tasks()]
    data = yaml.safe_load(open(tmp_path / "tasks.yml").read())
    assert "tests.test_api:DynamicTask" in data
    run = client.post("/tasks/dynamic/run")
    assert run.status_code == 200
    assert run.json()["result"] == "dyn"


def test_register_task_with_schedule(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post(
        "/tasks",
        params={"path": "tests.test_api:DynamicTask", "schedule": "*/5 * * * *"},
    )
    assert resp.status_code == 200
    job = sched.scheduler.get_job("DynamicTask")
    assert job is not None


def test_register_task_invalid_path(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post("/tasks", params={"path": "no.module:Missing"})
    assert resp.status_code == 400
    assert "no" in resp.json()["detail"]

