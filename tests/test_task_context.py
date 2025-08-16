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
