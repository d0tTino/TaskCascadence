import pytest

from task_cascadence.stage_store import StageStore
from task_cascadence.orchestrator import PrecheckError, TaskPipeline


class FailingTask:
    def run(self):
        raise RuntimeError("boom")


class SuccessTask:
    def run(self):
        return "done"


class PrecheckFailTask:
    def precheck(self):
        return False

    def run(self):
        return "unused"


class PartialOutputTask:
    def run(self):
        return {"partial": True}


def _init_store(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "stages.yml"))
    import task_cascadence.ume as ume

    ume._stage_store = None
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_stage_update_event", lambda *a, **k: None
    )
    return ume


def test_audit_log_persists_and_captures_failure(monkeypatch, tmp_path):
    ume = _init_store(monkeypatch, tmp_path)

    pipeline = TaskPipeline(FailingTask())
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


def test_audit_log_records_success_with_output(monkeypatch, tmp_path):
    _init_store(monkeypatch, tmp_path)

    pipeline = TaskPipeline(SuccessTask())
    assert pipeline.execute() == "done"

    store = StageStore()
    events = store.get_events("SuccessTask", category="audit")
    success = [e for e in events if e.get("status") == "success"]
    assert success and success[0]["output"] == repr("done")


def test_audit_log_records_precheck_failure(monkeypatch, tmp_path):
    _init_store(monkeypatch, tmp_path)

    pipeline = TaskPipeline(PrecheckFailTask())
    with pytest.raises(PrecheckError):
        pipeline.execute()

    store = StageStore()
    events = store.get_events("PrecheckFailTask", category="audit")
    failure = [e for e in events if e.get("status") == "error"]
    assert failure and failure[0]["reason"] == "precheck failed"
    assert failure[0].get("output") is None


def test_audit_log_records_partial_output_on_failure(monkeypatch, tmp_path):
    _init_store(monkeypatch, tmp_path)
    # trigger failure after result is computed
    def raise_emit(*a, **k):
        raise RuntimeError("emit fail")

    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_stage_update_event", raise_emit
    )

    pipeline = TaskPipeline(PartialOutputTask())
    with pytest.raises(RuntimeError, match="emit fail"):
        pipeline.execute()

    store = StageStore()
    events = store.get_events("PartialOutputTask", category="audit")
    failure = [e for e in events if e.get("status") == "error"]
    assert failure and failure[0]["reason"] == "emit fail"
    assert "partial" in failure[0]["output"]
