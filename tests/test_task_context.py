import asyncio
import threading
import time
import sys
from pathlib import Path
from types import ModuleType
import pytest

pkg = sys.modules.setdefault("task_cascadence", ModuleType("task_cascadence"))
pkg.__path__ = [str(Path(__file__).resolve().parents[1] / "task_cascadence")]

from task_cascadence.orchestrator import TaskPipeline  # noqa: E402
from task_cascadence.plugins import BaseTask  # noqa: E402


class ContextTask(BaseTask):
    name = "context"

    def run(self):
        self.seen_user_id = self.user_id
        self.seen_group_id = self.group_id
        return "ok"


def _patch_events(monkeypatch):
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_stage_update_event", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_audit_log", lambda *a, **k: None)


def test_task_pipeline_context(monkeypatch):
    _patch_events(monkeypatch)
    task = ContextTask()
    pipeline = TaskPipeline(task)
    pipeline.run(user_id="alice", group_id="team1")
    assert task.seen_user_id == "alice"
    assert task.seen_group_id == "team1"
    assert task.user_id == "alice"
    assert task.group_id == "team1"


def _capture_events(monkeypatch):
    stages: list[str] = []
    audits: list[tuple[str, str, str | None]] = []

    def stage(task_name, stage, *args, **kwargs):
        stages.append(stage)

    def audit(task_name, stage, status, *args, **kwargs):
        audits.append((stage, status, kwargs.get("output")))

    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_stage_update_event", stage)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_audit_log", audit)

    return stages, audits


def test_pipeline_consumes_attached_context(monkeypatch):
    stages, audits = _capture_events(monkeypatch)

    class ContextAwareTask(BaseTask):
        name = "context-aware"

        def __init__(self) -> None:
            self.context_history: list[tuple[str, list[object]]] = []

        def intake(self):
            self.context_history.append(("intake", list(getattr(self, "context", []))))

        def research(self):
            self.context_history.append(("research", list(self.context)))

        def plan(self):
            self.context_history.append(("plan", list(self.context)))
            return "plan"

        def run(self, plan):
            self.context_history.append(("run", list(self.context)))
            return "result"

        def verify(self, result):
            self.context_history.append(("verify", list(self.context)))
            return result

    task = ContextAwareTask()
    pipeline = TaskPipeline(task)

    pipeline.attach_context({"id": 1}, user_id="alice", group_id="team1")
    pipeline.pause(user_id="alice", group_id="team1")

    results: list[str] = []

    def _runner() -> None:
        results.append(pipeline.run(user_id="alice", group_id="team1"))

    thread = threading.Thread(target=_runner)
    thread.start()

    while len(task.context_history) < 1:
        time.sleep(0.01)

    pipeline.attach_context({"id": 2}, user_id="alice", group_id="team1")
    time.sleep(0.01)
    pipeline.resume(user_id="alice", group_id="team1")
    thread.join()

    assert results == ["result"]
    assert task.context_history == [
        ("intake", [{"id": 1}]),
        ("research", [{"id": 1}, {"id": 2}]),
        ("plan", [{"id": 1}, {"id": 2}]),
        ("run", [{"id": 1}, {"id": 2}]),
        ("verify", [{"id": 1}, {"id": 2}]),
    ]
    assert stages.count("context_attached") == 2
    assert ("context_attached", "received", repr({"id": 2})) in audits


@pytest.mark.asyncio
async def test_pipeline_consumes_attached_context_async(monkeypatch):
    stages, audits = _capture_events(monkeypatch)

    intake_event = asyncio.Event()

    class AsyncContextTask(BaseTask):
        name = "async-context"

        def __init__(self) -> None:
            self.context_history: list[tuple[str, list[object]]] = []

        def intake(self):
            self.context_history.append(("intake", list(getattr(self, "context", []))))
            intake_event.set()

        async def research(self):
            self.context_history.append(("research", list(self.context)))

        def plan(self):
            self.context_history.append(("plan", list(self.context)))
            return "plan"

        async def run(self, plan):
            self.context_history.append(("run", list(self.context)))
            return "result"

        def verify(self, result):
            self.context_history.append(("verify", list(self.context)))
            return result

    task = AsyncContextTask()
    pipeline = TaskPipeline(task)

    pipeline.attach_context("alpha", user_id="bob", group_id="team2")
    pipeline.pause(user_id="bob", group_id="team2")

    run_task = asyncio.create_task(pipeline.run_async(user_id="bob", group_id="team2"))
    await intake_event.wait()
    pipeline.attach_context("beta", user_id="bob", group_id="team2")
    pipeline.resume(user_id="bob", group_id="team2")
    result = await run_task

    assert result == "result"
    assert task.context_history == [
        ("intake", ["alpha"]),
        ("research", ["alpha", "beta"]),
        ("plan", ["alpha", "beta"]),
        ("run", ["alpha", "beta"]),
        ("verify", ["alpha", "beta"]),
    ]
    assert stages.count("context_attached") == 2
    assert ("context_attached", "received", "'beta'") in audits


@pytest.mark.asyncio
async def test_task_pipeline_context_async(monkeypatch):
    _patch_events(monkeypatch)
    task = ContextTask()
    pipeline = TaskPipeline(task)
    await pipeline.run_async(user_id="bob", group_id="team2")
    assert task.seen_user_id == "bob"
    assert task.seen_group_id == "team2"
    assert task.user_id == "bob"
    assert task.group_id == "team2"


def test_scheduler_sets_task_context(monkeypatch):
    from task_cascadence.scheduler import BaseScheduler

    class SimpleTask:
        def run(self):
            self.seen_user_id = self.user_id
            self.seen_group_id = self.group_id
            return "ok"

    monkeypatch.setattr(
        "task_cascadence.ume.emit_task_run", lambda *a, **k: None
    )
    sched = BaseScheduler()
    sched.register_task("simple", SimpleTask())
    sched.run_task("simple", user_id="alice", group_id="team1")
    task = sched._tasks["simple"]["task"]
    assert task.seen_user_id == "alice"
    assert task.seen_group_id == "team1"
    assert task.user_id == "alice"
    assert task.group_id == "team1"
