import threading
import time
from task_cascadence.pipeline_registry import (
    add_pipeline,
    remove_pipeline,
    list_pipelines,
    attach_pipeline_context,
    get_pipeline,
    get_latest_pipeline_for_task,
)
from task_cascadence.orchestrator import TaskPipeline
from task_cascadence.plugins import ExampleTask


def test_thread_safe_registry():
    pipeline = TaskPipeline(ExampleTask())
    errors: list[Exception] = []
    stop_event = threading.Event()

    def add_remove_worker(worker_id: int) -> None:
        # Add and remove multiple pipelines
        for i in range(100):
            name = f"p{worker_id}_{i}"
            run_id = f"{name}-run"
            add_pipeline(name, run_id, pipeline)
            time.sleep(0.001)
            remove_pipeline(name, run_id)
            task_name = f"task{worker_id}"
            run_id = f"{task_name}_run_{i}"
            add_pipeline(task_name, run_id, pipeline)
            time.sleep(0.001)
            remove_pipeline(run_id)
        if worker_id == 0:
            stop_event.set()

    def list_worker() -> None:
        # Continuously list pipelines while other threads modify registry
        while not stop_event.is_set():
            try:
                list_pipelines()
            except Exception as exc:  # pragma: no cover - failure information
                errors.append(exc)

    add_thread = threading.Thread(target=add_remove_worker, args=(0,))
    add_thread2 = threading.Thread(target=add_remove_worker, args=(1,))
    list_thread = threading.Thread(target=list_worker)

    add_thread.start()
    add_thread2.start()
    list_thread.start()

    add_thread.join()
    add_thread2.join()
    stop_event.set()
    list_thread.join()

    assert not errors
    assert list_pipelines() == {}


def test_attach_pipeline_context_exposes_pipeline(monkeypatch):
    stages: list[str] = []
    audits: list[tuple[str, str | None]] = []

    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)

    def stage(task_name, stage, *args, **kwargs):
        stages.append(stage)

    def audit(task_name, stage, status, *args, **kwargs):
        audits.append((stage, kwargs.get("output")))

    monkeypatch.setattr("task_cascadence.orchestrator.emit_stage_update_event", stage)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_audit_log", audit)

    class SimpleTask:
        def run(self):
            self.context_snapshot = list(getattr(self, "context", []))
            return "ok"

    first_pipeline = TaskPipeline(SimpleTask())
    first_run_id = "demo-run"
    first_pipeline.current_run_id = first_run_id
    add_pipeline("demo", first_run_id, first_pipeline)
    try:
        assert attach_pipeline_context(
            first_run_id, {"payload": 1}, user_id="carol", group_id="g"
        )
        result = first_pipeline.run(user_id="carol", group_id="g")
        assert result == "ok"
        assert first_pipeline.task.context_snapshot == [{"payload": 1}]
        assert stages.count("context_attached") == 1
        assert ("context_attached", "{'payload': 1}") in audits
    finally:
        remove_pipeline(first_run_id)

    second_pipeline = TaskPipeline(SimpleTask())
    second_run_id = "demo-run-2"
    second_pipeline.current_run_id = second_run_id
    add_pipeline("demo", second_run_id, second_pipeline)
    try:
        assert attach_pipeline_context(
            "demo", {"payload": 2}, user_id="carol", group_id="g", run_id=second_run_id
        )
        result = second_pipeline.run(user_id="carol", group_id="g")
        assert result == "ok"
        assert second_pipeline.task.context_snapshot == [{"payload": 2}]
    finally:
        remove_pipeline(second_run_id)

    assert attach_pipeline_context("missing", "value") is False


def test_pipeline_lookup_helpers():
    class DummyTask:
        def run(self):
            return "done"

    first_pipeline = TaskPipeline(DummyTask())
    second_pipeline = TaskPipeline(DummyTask())

    add_pipeline("demo", "run-1", first_pipeline)
    add_pipeline("demo", "run-2", second_pipeline)

    try:
        assert get_pipeline("run-1") is first_pipeline
        assert get_latest_pipeline_for_task("demo") is second_pipeline
        # Removing the latest should cause the previous run to be returned next
        remove_pipeline("run-2")
        assert get_latest_pipeline_for_task("demo") is first_pipeline
        # Fallback by task name should succeed while the first pipeline remains
        assert attach_pipeline_context("demo", {"payload": 2}) is True
    finally:
        remove_pipeline("run-1")

