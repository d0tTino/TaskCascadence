from typing import Any

from fastapi.testclient import TestClient

from task_cascadence.api import app
from task_cascadence.scheduler import CronScheduler
from task_cascadence.plugins import ExampleTask
from task_cascadence.stage_store import StageStore
from task_cascadence.ume import _hash_user_id, emit_audit_log, emit_stage_update_event
from task_cascadence.pipeline_registry import add_pipeline, get_pipeline, remove_pipeline
from task_cascadence.orchestrator import TaskPipeline
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


def test_api_pipeline_status(monkeypatch, tmp_path, auth_headers):
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "stages.yml"))
    import task_cascadence.ume as ume

    ume._stage_store = None
    user_hash = _hash_user_id("bob")
    group_hash = _hash_user_id("builders")
    StageStore().add_event("example", "start", user_hash, group_id=group_hash)
    StageStore().add_event("example", "finish", user_hash, group_id=group_hash)
    StageStore().add_event(
        "example", "other", _hash_user_id("alice"), group_id=_hash_user_id("artists")
    )
    events = StageStore().get_events("example", user_hash=user_hash, group_id=group_hash)

    emit_stage_update_event("example", "queued", user_id="carol", group_id="crew")
    raw_group_events = StageStore().get_events(
        "example", user_hash=_hash_user_id("carol"), group_id="crew"
    )

    setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)

    resp = client.get(
        "/pipeline/example",
        headers=auth_headers("bob", group_hash),
    )
    assert resp.status_code == 200
    assert resp.json() == events

    resp = client.get(
        "/pipeline/example",
        headers=auth_headers("carol", "crew"),
    )
    assert resp.status_code == 200
    assert resp.json() == raw_group_events

    resp = client.get("/pipeline/example", headers={"X-Group-ID": "team"})
    assert resp.status_code == 400

    resp = client.get("/pipeline/example", headers={"X-User-ID": "alice"})
    assert resp.status_code == 400


def test_api_pipeline_audit(monkeypatch, tmp_path, auth_headers):
    path = tmp_path / "audit.yml"
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(path))
    import task_cascadence.ume as ume

    ume._stage_store = None
    store = StageStore()
    user_hash = _hash_user_id("user-a")
    group_hash = _hash_user_id("alpha")
    other_user_hash = _hash_user_id("user-b")
    other_group_hash = _hash_user_id("beta")
    store.add_event(
        "example",
        "submitted",
        user_hash,
        group_id=group_hash,
        category="audit",
    )
    store.add_event(
        "example",
        "approved",
        user_hash,
        group_id=group_hash,
        category="audit",
    )
    store.add_event(
        "example",
        "other",
        other_user_hash,
        group_id=other_group_hash,
        category="audit",
    )

    scoped_events = store.get_events(
        "example", user_hash=user_hash, group_id=group_hash, category="audit"
    )

    client = TestClient(app)

    headers = auth_headers("user-a", group_hash)

    resp = client.get("/pipeline/example/audit", headers=headers)
    assert resp.status_code == 200
    assert resp.json() == scoped_events

    resp = client.get(
        "/pipeline/example/audit",
        params={"user_hash": "user-a"},
        headers=headers,
    )
    assert resp.status_code == 200
    assert resp.json() == scoped_events

    emit_audit_log(
        "example",
        "reviewed",
        "ok",
        user_id="carol",
        group_id="crew",
    )
    raw_group_audit = StageStore().get_events(
        "example", user_hash=_hash_user_id("carol"), group_id="crew", category="audit"
    )

    resp = client.get(
        "/pipeline/example/audit",
        headers=auth_headers("carol", "crew"),
    )
    assert resp.status_code == 200
    assert resp.json() == raw_group_audit

    resp = client.get(
        "/pipeline/example/audit",
        params={"group_id": group_hash},
        headers=headers,
    )
    assert resp.status_code == 200
    assert resp.json() == scoped_events

    resp = client.get(
        "/pipeline/example/audit",
        params={"group_id": "beta"},
        headers=headers,
    )
    assert resp.status_code == 403

    resp = client.get(
        "/pipeline/example/audit",
        params={"user_hash": "user-b"},
        headers=headers,
    )
    assert resp.status_code == 403

    resp = client.get("/pipeline/example/audit", headers={"X-Group-ID": "team"})
    assert resp.status_code == 400

    resp = client.get("/pipeline/example/audit", headers={"X-User-ID": "alice"})
    assert resp.status_code == 400


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


def _start_context_signal_pipeline(monkeypatch, tmp_path):
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
    pipeline = get_pipeline("contextsignal")
    assert pipeline is not None
    run_id = pipeline.current_run_id
    assert run_id is not None

    return client, task, run_id, thread


def test_api_context_signal(monkeypatch, tmp_path):
    client, task, run_id, thread = _start_context_signal_pipeline(monkeypatch, tmp_path)

    resp = client.post(
        f"/tasks/{run_id}/signal",
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
        event.get("stage") == "context_attached"
        and event.get("status") == "received"
        for event in audit_events
    )
    assert any(
        event.get("stage") == "plan.context"
        and event.get("status") == "consumed"
        for event in audit_events
    )

    resp = client.post(
        f"/tasks/{run_id}/signal",
        headers={"X-User-ID": "alice", "X-Group-ID": "team"},
        json={"kind": "context", "value": {"note": "later"}},
    )
    assert resp.status_code == 404


def test_api_context_signal_rejects_large_payload(monkeypatch, tmp_path):
    client, task, run_id, thread = _start_context_signal_pipeline(monkeypatch, tmp_path)

    resp = client.post(
        f"/tasks/{run_id}/signal",
        headers={"X-User-ID": "alice", "X-Group-ID": "team"},
        json={"kind": "context", "value": {"note": "a" * 1800, "url": "b" * 1500}},
    )

    assert resp.status_code == 400
    assert "too large" in resp.json().get("detail", "")
    assert not task.contexts

    resp = client.post(
        f"/tasks/{run_id}/signal",
        headers={"X-User-ID": "alice", "X-Group-ID": "team"},
        json={"kind": "context", "value": {"note": "ok"}},
    )
    assert resp.status_code == 202

    task.allow_plan.set()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert task.contexts == [{"note": "ok"}]


def test_api_context_signal_rejects_invalid_payload(monkeypatch, tmp_path):
    client, task, run_id, thread = _start_context_signal_pipeline(monkeypatch, tmp_path)

    resp = client.post(
        f"/tasks/{run_id}/signal",
        headers={"X-User-ID": "alice", "X-Group-ID": "team"},
        json={"kind": "context", "value": {"note": None}},
    )
    assert resp.status_code == 400

    resp = client.post(
        f"/tasks/{run_id}/signal",
        headers={"X-User-ID": "alice", "X-Group-ID": "team"},
        json={"kind": "context", "value": {"note": "valid"}},
    )
    assert resp.status_code == 202

    task.allow_plan.set()
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert task.contexts == [{"note": "valid"}]


def test_api_context_signal_redacts_audit(monkeypatch, tmp_path):
    client, task, run_id, thread = _start_context_signal_pipeline(monkeypatch, tmp_path)

    long_note = "x" * 600
    resp = client.post(
        f"/tasks/{run_id}/signal",
        headers={"X-User-ID": "alice", "X-Group-ID": "team"},
        json={"kind": "context", "value": {"note": long_note}},
    )
    assert resp.status_code == 202

    task.allow_plan.set()
    thread.join(timeout=2)
    assert not thread.is_alive()

    store = StageStore(path=tmp_path / "sig_stages.yml")
    audit_events = store.get_events(task.__class__.__name__, category="audit")
    note_outputs = [e.get("output", "") for e in audit_events]
    assert any(len(out or "") < len(repr({"note": long_note})) for out in note_outputs)


def test_parallel_context_signals_route_to_matching_pipeline(monkeypatch):
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_stage_update_event", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_audit_log", lambda *a, **k: None)

    class ParallelContextTask(ExampleTask):
        name = "parallel-context"

        def __init__(self) -> None:
            super().__init__()
            self.research_ready = threading.Event()
            self.allow_plan = threading.Event()
            self.plan_contexts: list[list[dict[str, Any]]] = []
            self.run_contexts: list[list[dict[str, Any]]] = []

        def intake(self) -> None:
            pass

        def research(self) -> None:
            self.research_ready.set()
            if not self.allow_plan.wait(timeout=1):
                raise AssertionError("research did not resume")

        def plan(self) -> dict[str, Any]:
            snapshot = list(self.context)
            self.plan_contexts.append(snapshot)
            return snapshot[-1] if snapshot else {}

        def run(self, plan_result: dict[str, Any]) -> dict[str, Any]:
            self.run_contexts.append(list(self.context))
            return plan_result

    task_one = ParallelContextTask()
    task_two = ParallelContextTask()

    pipeline_one = TaskPipeline(task_one)
    pipeline_two = TaskPipeline(task_two)

    run_one = "run-one"
    run_two = "run-two"
    pipeline_one.current_run_id = run_one
    pipeline_two.current_run_id = run_two

    add_pipeline("parallel-context", run_one, pipeline_one)
    add_pipeline("parallel-context", run_two, pipeline_two)

    client = TestClient(app)

    thread_one = threading.Thread(
        target=lambda: pipeline_one.run(user_id="alice", group_id="team"), daemon=True
    )
    thread_two = threading.Thread(
        target=lambda: pipeline_two.run(user_id="bob", group_id="ops"), daemon=True
    )

    thread_one.start()
    thread_two.start()

    try:
        assert task_one.research_ready.wait(timeout=1)
        assert task_two.research_ready.wait(timeout=1)

        resp_one = client.post(
            f"/tasks/{run_one}/signal",
            headers={"X-User-ID": "alice", "X-Group-ID": "team"},
            json={"kind": "context", "value": {"note": "one"}},
        )
        resp_two = client.post(
            f"/tasks/{run_two}/signal",
            headers={"X-User-ID": "bob", "X-Group-ID": "ops"},
            json={"kind": "context", "value": {"note": "two"}},
        )

        assert resp_one.status_code == 202
        assert resp_two.status_code == 202

        task_one.allow_plan.set()
        task_two.allow_plan.set()

        thread_one.join(timeout=2)
        thread_two.join(timeout=2)

        assert not thread_one.is_alive()
        assert not thread_two.is_alive()

        assert task_one.plan_contexts == [[{"note": "one"}]]
        assert task_two.plan_contexts == [[{"note": "two"}]]
        assert task_one.run_contexts == [[{"note": "one"}]]
        assert task_two.run_contexts == [[{"note": "two"}]]
    finally:
        remove_pipeline(run_one, task_name="parallel-context")
        remove_pipeline(run_two, task_name="parallel-context")
