import pytest

from task_cascadence.stage_store import StageStore
from task_cascadence.orchestrator import TaskPipeline


class FailingTask:
    def run(self):
        raise RuntimeError("boom")


def test_audit_log_persists_and_captures_failure(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "stages.yml"))
    # reset singleton cache
    import task_cascadence.ume as ume

    ume._stage_store = None

    pipeline = TaskPipeline(FailingTask())
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None
    )
    with pytest.raises(RuntimeError):
        pipeline.execute()

    store = StageStore()
    events = store.get_events("FailingTask", category="audit")
    # ensure failure recorded
    failure = [e for e in events if e.get("status") == "error"]
    assert failure and "boom" in failure[0]["reason"]

    # simulate restart by creating new store instance
    ume._stage_store = None
    store2 = StageStore()
    assert store2.get_events("FailingTask", category="audit") == events
