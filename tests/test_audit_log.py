import importlib.machinery
import importlib.util
import pathlib
import sys
import types

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
spec_pkg = importlib.machinery.ModuleSpec(
    "task_cascadence", loader=None, is_package=True
)
spec_pkg.submodule_search_locations = [str(ROOT / "task_cascadence")]
pkg = importlib.util.module_from_spec(spec_pkg)
sys.modules.setdefault("task_cascadence", pkg)

spec_ume_pkg = importlib.machinery.ModuleSpec(
    "task_cascadence.ume", loader=None, is_package=True
)
spec_ume_pkg.submodule_search_locations = [str(ROOT / "task_cascadence" / "ume")]
ume_pkg = importlib.util.module_from_spec(spec_ume_pkg)
sys.modules.setdefault("task_cascadence.ume", ume_pkg)


def _load(name: str, path: pathlib.Path) -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[name] = module
    spec.loader.exec_module(module)
    if "." in name:
        parent_name, attr = name.rsplit(".", 1)
        parent = sys.modules[parent_name]
        setattr(parent, attr, module)
    return module


_load("task_cascadence.config", ROOT / "task_cascadence" / "config.py")
_load("task_cascadence.ume.models", ROOT / "task_cascadence" / "ume" / "models.py")
_load("task_cascadence.idea_store", ROOT / "task_cascadence" / "idea_store.py")
_load("task_cascadence.suggestion_store", ROOT / "task_cascadence" / "suggestion_store.py")
stage_store = _load("task_cascadence.stage_store", ROOT / "task_cascadence" / "stage_store.py")
StageStore = stage_store.StageStore
_load("task_cascadence.transport", ROOT / "task_cascadence" / "transport.py")
_load("task_cascadence.ume", ROOT / "task_cascadence" / "ume" / "__init__.py")
_load("task_cascadence.research", ROOT / "task_cascadence" / "research.py")
orchestrator = _load("task_cascadence.orchestrator", ROOT / "task_cascadence" / "orchestrator.py")
PrecheckError = orchestrator.PrecheckError
TaskPipeline = orchestrator.TaskPipeline


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


class PrecheckPassTask:
    def precheck(self):
        return True

    def run(self):
        return "ok"


class PartialOutputTask:
    def run(self):
        return {"partial": True}


class IntakePartialErrorTask:
    def intake(self):
        exc = RuntimeError("intake fail")
        exc.partial = {"stage": "intake"}
        raise exc


class ResearchPartialErrorTask:
    def research(self):
        return "topic"


class PlanPartialErrorTask:
    def plan(self):
        exc = RuntimeError("plan fail")
        exc.partial = ["step1"]
        raise exc


class VerifyPartialErrorTask:
    def run(self):
        return "result"

    def verify(self, exec_result):
        exc = RuntimeError("verify fail")
        exc.partial = {"verified": False}
        raise exc


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
        pipeline.execute(user_id="alice")

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
    assert pipeline.execute(user_id="alice") == "done"

    store = StageStore()
    events = store.get_events("SuccessTask", category="audit")
    success = [e for e in events if e.get("status") == "success"]
    assert success and success[0]["output"] == repr("done")


def test_audit_log_records_precheck_failure(monkeypatch, tmp_path):
    _init_store(monkeypatch, tmp_path)

    pipeline = TaskPipeline(PrecheckFailTask())
    with pytest.raises(PrecheckError):
        pipeline.execute(user_id="alice")

    store = StageStore()
    events = store.get_events("PrecheckFailTask", category="audit")
    started = [e for e in events if e.get("stage") == "precheck" and e.get("status") == "started"]
    failure = [e for e in events if e.get("stage") == "precheck" and e.get("status") == "error"]
    assert started and failure
    assert failure[0]["reason"] == "precheck failed"
    assert failure[0].get("output") is None


def test_audit_log_records_precheck_success(monkeypatch, tmp_path):
    _init_store(monkeypatch, tmp_path)

    pipeline = TaskPipeline(PrecheckPassTask())
    assert pipeline.execute(user_id="alice") == "ok"

    store = StageStore()
    events = store.get_events("PrecheckPassTask", category="audit")
    started = [e for e in events if e.get("stage") == "precheck" and e.get("status") == "started"]
    success = [e for e in events if e.get("stage") == "precheck" and e.get("status") == "success"]
    assert started and success


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
        pipeline.execute(user_id="alice")

    store = StageStore()
    events = store.get_events("PartialOutputTask", category="audit")
    failure = [e for e in events if e.get("status") == "error"]
    assert failure and failure[0]["reason"] == "emit fail"
    assert "partial" in failure[0]["output"]


def test_audit_log_records_partial_output_on_intake_failure(monkeypatch, tmp_path):
    _init_store(monkeypatch, tmp_path)
    pipeline = TaskPipeline(IntakePartialErrorTask())
    with pytest.raises(RuntimeError, match="intake fail"):
        pipeline.intake(user_id="alice")

    store = StageStore()
    events = store.get_events("IntakePartialErrorTask", category="audit")
    failure = [e for e in events if e.get("status") == "error"]
    assert failure and "intake" in failure[0]["output"]


def test_audit_log_records_partial_output_on_research_failure(monkeypatch, tmp_path):
    _init_store(monkeypatch, tmp_path)

    def failing_gather(query, *, user_id=None, group_id=None):
        exc = RuntimeError("gather fail")
        exc.partial = {"topic": query}
        raise exc

    monkeypatch.setattr(
        "task_cascadence.research.gather", failing_gather
    )

    pipeline = TaskPipeline(ResearchPartialErrorTask())
    pipeline.research(user_id="alice")

    store = StageStore()
    events = store.get_events("ResearchPartialErrorTask", category="audit")
    failure = [e for e in events if e.get("status") == "error"]
    assert failure and "topic" in failure[0]["output"]


def test_audit_log_records_partial_output_on_plan_failure(monkeypatch, tmp_path):
    _init_store(monkeypatch, tmp_path)
    pipeline = TaskPipeline(PlanPartialErrorTask())
    with pytest.raises(RuntimeError, match="plan fail"):
        pipeline.plan(user_id="alice")

    store = StageStore()
    events = store.get_events("PlanPartialErrorTask", category="audit")
    failure = [e for e in events if e.get("status") == "error"]
    assert failure and "step1" in failure[0]["output"]


def test_audit_log_records_partial_output_on_verify_failure(monkeypatch, tmp_path):
    _init_store(monkeypatch, tmp_path)
    pipeline = TaskPipeline(VerifyPartialErrorTask())
    with pytest.raises(RuntimeError, match="verify fail"):
        pipeline.verify("result", user_id="alice")

    store = StageStore()
    events = store.get_events("VerifyPartialErrorTask", category="audit")
    failure = [e for e in events if e.get("status") == "error"]
    assert failure and "verified" in failure[0]["output"]
