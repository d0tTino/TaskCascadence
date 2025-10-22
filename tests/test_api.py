from fastapi.testclient import TestClient

import tempfile

from task_cascadence.api import app
from task_cascadence.scheduler import CronScheduler, TaskExecutionResult
from task_cascadence.plugins import CronTask
import yaml


REQUIRED_HEADERS = {"X-User-ID": "alice", "X-Group-ID": "team"}
USER_ONLY_HEADERS = {"X-User-ID": "alice"}


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


class PipelineTask(CronTask):
    name = "pipe"

    def intake(self):
        pass

    def run(self):
        return "pipe"


class AsyncPipelineTask(CronTask):
    name = "asyncpipe"

    def intake(self):
        pass

    async def run(self):
        return "asyncpipe"


def setup_scheduler(monkeypatch, tmp_path):
    sched = CronScheduler(storage_path=tmp_path / "sched.yml")
    task = DummyTask()
    sched.register_task(name_or_task="dummy", task_or_expr=task)
    monkeypatch.setattr("task_cascadence.api.get_default_scheduler", lambda: sched)
    monkeypatch.setattr(
        "task_cascadence.ume.emit_task_run",
        lambda run, user_id=None, group_id=None: None,
    )
    tmp = tempfile.NamedTemporaryFile(delete=False)
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", tmp.name)
    monkeypatch.setenv("CASCADENCE_TASKS_PATH", str(tmp_path / "tasks.yml"))
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "stages.yml"))
    return sched, task


def test_list_tasks(monkeypatch, tmp_path):
    sched, _task = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.get(
        "/tasks",
        headers={"X-User-ID": "alice", "X-Group-ID": "team"},
    )
    resp = client.get("/tasks", headers=REQUIRED_HEADERS)
    assert resp.status_code == 200
    assert resp.json() == [{"name": "dummy", "disabled": False}]


def test_list_tasks_missing_user_header(monkeypatch, tmp_path):
    setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.get("/tasks", headers={"X-Group-ID": "team"})
    assert resp.status_code == 400


def test_list_tasks_missing_group_header(monkeypatch, tmp_path):
    setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.get("/tasks", headers={"X-User-ID": "alice"})
    assert resp.status_code == 400


def test_list_tasks_emits_audit_log(monkeypatch, tmp_path):
    setup_scheduler(monkeypatch, tmp_path)
    calls: dict[str, tuple[str, str, str]] = {}

    def fake_emit(task_name, stage, status, **kwargs):
        calls["args"] = (task_name, stage, status)
        calls["kwargs"] = kwargs

    monkeypatch.setattr("task_cascadence.api.emit_audit_log", fake_emit)
    client = TestClient(app)
    resp = client.get(
        "/tasks",
        headers={"X-User-ID": "alice", "X-Group-ID": "team"},
    )

    assert resp.status_code == 200
    assert calls["args"] == ("api", "tasks", "list")
    assert calls["kwargs"]["user_id"] == "alice"
    assert calls["kwargs"]["group_id"] == "team"
    assert calls["kwargs"]["output"] == "1"


def test_list_tasks_missing_group_header(monkeypatch, tmp_path):
    setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.get("/tasks", headers=USER_ONLY_HEADERS)
    assert resp.status_code == 400


def test_run_task(monkeypatch, tmp_path):
    sched, task = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post(
        "/tasks/dummy/run",
        headers=REQUIRED_HEADERS,
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["result"] == "ok"
    assert data["run_id"]
    assert task.ran == 1


def test_run_task_missing_group_header(monkeypatch, tmp_path):
    setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post("/tasks/dummy/run", headers=USER_ONLY_HEADERS)
    assert resp.status_code == 400


def test_run_task_async_endpoint(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    async_task = AsyncTask()
    sched.register_task(name_or_task="async", task_or_expr=async_task)
    client = TestClient(app)
    resp = client.post(
        "/tasks/async/run-async",
        headers=REQUIRED_HEADERS,
    )

    assert resp.status_code == 200
    data = resp.json()
    assert data["result"] == "async"
    assert data["run_id"]


def test_run_task_user_header(monkeypatch, tmp_path):
    called = {}

    def fake_run_with_metadata(name, use_temporal=False, user_id=None, group_id=None):
        called["uid"] = user_id
        return TaskExecutionResult(run_id="test-run", result="r")

    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    monkeypatch.setattr(sched, "run_task_with_metadata", fake_run_with_metadata)
    client = TestClient(app)
    client.post(
        "/tasks/dummy/run",
        headers=REQUIRED_HEADERS,
    )
    assert called["uid"] == "alice"


def test_run_task_group_header(monkeypatch, tmp_path):
    called = {}

    def fake_run_with_metadata(name, use_temporal=False, user_id=None, group_id=None):
        called["gid"] = group_id
        return TaskExecutionResult(run_id="test-run", result="r")

    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    monkeypatch.setattr(sched, "run_task_with_metadata", fake_run_with_metadata)
    client = TestClient(app)
    client.post(
        "/tasks/dummy/run",
        headers=REQUIRED_HEADERS,
    )
    assert called["gid"] == "team"


def test_stage_update_event_includes_headers(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    pipeline = PipelineTask()
    sched.register_task(name_or_task="pipe", task_or_expr=pipeline)
    events: list[dict[str, str]] = []

    def fake_emit(task_name, stage, **kwargs):
        events.append(kwargs)

    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_stage_update_event", fake_emit
    )
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_audit_log", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None
    )

    client = TestClient(app)
    headers = REQUIRED_HEADERS
    resp = client.post("/tasks/pipe/run", headers=headers)
    assert resp.status_code == 200
    run_id = resp.json()["run_id"]
    assert events
    assert all(
        e["user_id"] == "alice"
        and e["group_id"] == "team"
        and e.get("run_id") == run_id
        for e in events
    )


def test_stage_update_event_includes_headers_async(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    pipeline = AsyncPipelineTask()
    sched.register_task(name_or_task="asyncpipe", task_or_expr=pipeline)
    events: list[dict[str, str]] = []

    def fake_emit(task_name, stage, **kwargs):
        events.append(kwargs)

    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_stage_update_event", fake_emit
    )
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_audit_log", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None
    )

    client = TestClient(app)
    headers = REQUIRED_HEADERS
    resp = client.post("/tasks/asyncpipe/run-async", headers=headers)
    assert resp.status_code == 200
    run_id = resp.json()["run_id"]
    assert events
    assert all(
        e["user_id"] == "alice"
        and e["group_id"] == "team"
        and e.get("run_id") == run_id
        for e in events
    )


def test_schedule_task_group_header(monkeypatch, tmp_path):
    called = {}

    def fake_register(name_or_task, task_or_expr, user_id=None, group_id=None):
        called["gid"] = group_id

    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    monkeypatch.setattr(sched, "register_task", fake_register)
    client = TestClient(app)
    client.post(
        "/tasks/dummy/schedule",
        params={"expression": "*/5 * * * *"},
        headers=REQUIRED_HEADERS,
    )
    assert called["gid"] == "team"


def test_schedule_task(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post(
        "/tasks/dummy/schedule",
        params={"expression": "*/5 * * * *"},
        headers=REQUIRED_HEADERS,
    )
    assert resp.status_code == 200
    job = sched.scheduler.get_job("DummyTask")
    assert job is not None


def test_schedule_task_missing_group_header(monkeypatch, tmp_path):
    setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post(
        "/tasks/dummy/schedule",
        params={"expression": "*/5 * * * *"},
        headers=USER_ONLY_HEADERS,
    )
    assert resp.status_code == 400


def test_disable_task_missing_headers(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post("/tasks/dummy/disable", headers=USER_ONLY_HEADERS)

    assert resp.status_code == 400
    assert sched._tasks["dummy"]["disabled"] is False


def test_disable_task(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post(
        "/tasks/dummy/disable",
        headers=REQUIRED_HEADERS,
    )

    assert resp.status_code == 200
    assert sched._tasks["dummy"]["disabled"] is True


def test_pause_task_missing_headers(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post("/tasks/dummy/pause")
    assert resp.status_code == 400
    assert sched._tasks["dummy"]["paused"] is False


def test_pause_task_missing_group_header(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post("/tasks/dummy/pause", headers=USER_ONLY_HEADERS)
    assert resp.status_code == 400
    assert sched._tasks["dummy"]["paused"] is False


def test_pause_task(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    headers = REQUIRED_HEADERS
    resp = client.post("/tasks/dummy/pause", headers=headers)
    assert resp.status_code == 200
    assert sched._tasks["dummy"]["paused"] is True


def test_resume_task_missing_headers(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    headers = REQUIRED_HEADERS
    client.post("/tasks/dummy/pause", headers=headers)
    resp = client.post("/tasks/dummy/resume", headers=USER_ONLY_HEADERS)
    assert resp.status_code == 400
    assert sched._tasks["dummy"]["paused"] is True


def test_resume_task(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    headers = REQUIRED_HEADERS
    client.post("/tasks/dummy/pause", headers=headers)
    resp = client.post("/tasks/dummy/resume", headers=headers)
    assert resp.status_code == 200
    assert sched._tasks["dummy"]["paused"] is False


def test_register_task(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post(
        "/tasks",
        params={"path": "tests.test_api:DynamicTask"},
        headers=REQUIRED_HEADERS,
    )
    assert resp.status_code == 200
    assert "dynamic" in [name for name, _ in sched.list_tasks()]
    yaml.safe_load(open(tmp_path / "tasks.yml").read())
    run = client.post(
        "/tasks/dynamic/run",
        headers=REQUIRED_HEADERS,
    )

    assert run.status_code == 200
    assert run.json()["result"] == "dyn"


def test_register_task_with_schedule(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post(
        "/tasks",
        params={"path": "tests.test_api:DynamicTask", "schedule": "*/5 * * * *"},
        headers=REQUIRED_HEADERS,
    )
    assert resp.status_code == 200
    job = sched.scheduler.get_job("DynamicTask")
    assert job is not None


def test_register_task_invalid_path(monkeypatch, tmp_path):
    sched, _ = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post(
        "/tasks",
        params={"path": "no.module:Missing"},
        headers=REQUIRED_HEADERS,
    )
    assert resp.status_code == 400
    assert "no" in resp.json()["detail"]


def test_register_task_missing_user_header(monkeypatch, tmp_path):
    setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post(
        "/tasks",
        params={"path": "tests.test_api:DynamicTask"},
        headers={"X-Group-ID": "team"},
    )
    assert resp.status_code == 400


def test_register_task_missing_group_header(monkeypatch, tmp_path):
    setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post(
        "/tasks",
        params={"path": "tests.test_api:DynamicTask"},
        headers=USER_ONLY_HEADERS,
    )
    assert resp.status_code == 400

