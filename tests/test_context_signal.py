import asyncio
import threading
from typing import Any

import pytest
from fastapi.testclient import TestClient

from task_cascadence.api import app
from task_cascadence.orchestrator import TaskPipeline
from task_cascadence.plugins import BaseTask
from task_cascadence.pipeline_registry import get_pipeline
from task_cascadence.scheduler import CronScheduler


def _silence_pipeline_events(monkeypatch: pytest.MonkeyPatch) -> None:
    """Disable telemetry hooks that rely on background services."""

    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_stage_update_event", lambda *a, **k: None
    )
    monkeypatch.setattr("task_cascadence.orchestrator.emit_audit_log", lambda *a, **k: None)


class SyncContextTask(BaseTask):
    name = "sync-context"

    def __init__(self) -> None:
        self.context_history: list[tuple[str, list[Any]]] = []
        self.research_ready = threading.Event()
        self.resume_research = threading.Event()

    def intake(self) -> None:
        self.context_history.append(("intake", list(getattr(self, "context", []))))

    def research(self) -> str:
        self.context_history.append(("research", list(self.context)))
        self.research_ready.set()
        if not self.resume_research.wait(timeout=1):  # pragma: no cover - safety
            raise AssertionError("research did not resume")
        return "researched"

    def plan(self) -> dict[str, Any]:
        snapshot = list(self.context)
        self.context_history.append(("plan", snapshot))
        return {"context": snapshot[-1] if snapshot else None}

    def run(self, plan_result: dict[str, Any]) -> dict[str, Any]:
        self.context_history.append(("run", list(self.context)))
        return plan_result

    def verify(self, result: dict[str, Any]) -> dict[str, Any]:
        self.context_history.append(("verify", list(self.context)))
        return result


def test_pipeline_delivers_mid_run_context(monkeypatch: pytest.MonkeyPatch) -> None:
    _silence_pipeline_events(monkeypatch)
    task = SyncContextTask()
    pipeline = TaskPipeline(task)

    thread = threading.Thread(
        target=lambda: pipeline.run(user_id="alice", group_id="team"), daemon=True
    )
    thread.start()

    assert task.research_ready.wait(timeout=1)
    pipeline.attach_context({"note": "hello"}, user_id="alice", group_id="team")
    task.resume_research.set()
    thread.join(timeout=2)
    assert not thread.is_alive()

    assert task.context_history == [
        ("intake", []),
        ("research", []),
        ("plan", [{"note": "hello"}]),
        ("run", [{"note": "hello"}]),
        ("verify", [{"note": "hello"}]),
    ]


class AsyncContextTask(BaseTask):
    name = "async-context"

    def __init__(self) -> None:
        self.context_history: list[tuple[str, list[Any]]] = []
        self.research_ready = asyncio.Event()
        self.resume_research = asyncio.Event()

    def intake(self) -> None:
        self.context_history.append(("intake", list(getattr(self, "context", []))))

    async def research(self) -> str:
        self.context_history.append(("research", list(self.context)))
        self.research_ready.set()
        await asyncio.wait_for(self.resume_research.wait(), timeout=1)
        return "researched"

    def plan(self) -> dict[str, Any]:
        snapshot = list(self.context)
        self.context_history.append(("plan", snapshot))
        return {"context": snapshot[-1] if snapshot else None}

    async def run(self, plan_result: dict[str, Any]) -> dict[str, Any]:
        self.context_history.append(("run", list(self.context)))
        return plan_result

    def verify(self, result: dict[str, Any]) -> dict[str, Any]:
        self.context_history.append(("verify", list(self.context)))
        return result


@pytest.mark.asyncio
async def test_pipeline_delivers_mid_run_context_async(monkeypatch: pytest.MonkeyPatch) -> None:
    _silence_pipeline_events(monkeypatch)
    task = AsyncContextTask()
    pipeline = TaskPipeline(task)

    run_task = asyncio.create_task(pipeline.run_async(user_id="bob", group_id="builders"))

    await asyncio.wait_for(task.research_ready.wait(), timeout=1)
    pipeline.attach_context({"note": "async"}, user_id="bob", group_id="builders")
    task.resume_research.set()
    result = await asyncio.wait_for(run_task, timeout=2)
    assert result == {"context": {"note": "async"}}

    assert task.context_history == [
        ("intake", []),
        ("research", []),
        ("plan", [{"note": "async"}]),
        ("run", [{"note": "async"}]),
        ("verify", [{"note": "async"}]),
    ]


class ApiContextTask(BaseTask):
    name = "api-context"

    def __init__(self) -> None:
        self.research_started = threading.Event()
        self.allow_plan = threading.Event()
        self.plan_contexts: list[list[dict[str, Any]]] = []
        self.run_contexts: list[list[dict[str, Any]]] = []
        self.verify_contexts: list[list[dict[str, Any]]] = []

    def intake(self) -> None:
        pass

    def research(self) -> str:
        self.research_started.set()
        if not self.allow_plan.wait(timeout=1):  # pragma: no cover - safety
            raise AssertionError("plan stage timed out")
        return "ready"

    def plan(self) -> dict[str, Any]:
        snapshot = list(self.context)
        self.plan_contexts.append(snapshot)
        return snapshot[-1] if snapshot else {}

    def run(self, plan_result: dict[str, Any]) -> dict[str, Any]:
        self.run_contexts.append(list(self.context))
        return plan_result

    def verify(self, result: dict[str, Any]) -> dict[str, Any]:
        self.verify_contexts.append(list(self.context))
        return result


def _setup_api_scheduler(monkeypatch: pytest.MonkeyPatch, tmp_path) -> tuple[CronScheduler, ApiContextTask]:
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "stages.yml"))
    import task_cascadence.ume as ume

    ume._stage_store = None

    _silence_pipeline_events(monkeypatch)
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda *a, **k: None)

    sched = CronScheduler(storage_path=tmp_path / "sched.yml")
    task = ApiContextTask()
    sched.register_task("api-context", task)
    monkeypatch.setattr("task_cascadence.api.get_default_scheduler", lambda: sched)
    return sched, task


def test_api_context_signal_delivery(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    sched, task = _setup_api_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)

    thread = threading.Thread(
        target=lambda: sched.run_task("api-context", user_id="alice", group_id="team"),
        daemon=True,
    )
    thread.start()

    assert task.research_started.wait(timeout=1)
    pipeline = get_pipeline("api-context")
    assert pipeline is not None
    run_id = pipeline.current_run_id

    resp = client.post(
        f"/tasks/{run_id}/signal",
        headers={"X-User-ID": "alice", "X-Group-ID": "team"},
        json={"kind": "context", "value": {"note": "hello"}},
    )
    assert resp.status_code == 202
    assert resp.json() == {"status": "accepted"}

    task.allow_plan.set()
    thread.join(timeout=2)
    assert not thread.is_alive()

    assert task.plan_contexts == [[{"note": "hello"}]]
    assert task.run_contexts == [[{"note": "hello"}]]
    assert task.verify_contexts == [[{"note": "hello"}]]
    assert get_pipeline("api-context") is None


def test_api_context_signal_rejects_unsupported_kind(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    sched, task = _setup_api_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)

    thread = threading.Thread(
        target=lambda: sched.run_task("api-context", user_id="alice", group_id="team"),
        daemon=True,
    )
    thread.start()

    assert task.research_started.wait(timeout=1)

    pipeline = get_pipeline("api-context")
    assert pipeline is not None
    run_id = pipeline.current_run_id

    resp = client.post(
        f"/tasks/{run_id}/signal",
        headers={"X-User-ID": "alice", "X-Group-ID": "team"},
        json={"kind": "unknown", "value": {}},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"] == "unsupported signal kind"

    task.allow_plan.set()
    thread.join(timeout=2)
    assert not thread.is_alive()


@pytest.mark.parametrize(
    "headers, expected",
    [
        ({"X-Group-ID": "team"}, "user_id header required"),
        ({"X-User-ID": "alice"}, "group_id header required"),
    ],
)
def test_api_context_signal_missing_headers(headers, expected) -> None:
    client = TestClient(app)
    resp = client.post(
        "/tasks/demo-run/signal",
        headers=headers,
        json={"kind": "context", "value": {}},
    )
    assert resp.status_code == 400
    assert resp.json()["detail"] == expected
