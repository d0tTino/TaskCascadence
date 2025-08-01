import threading
import time
from task_cascadence.pipeline_registry import add_pipeline, remove_pipeline, list_pipelines
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
            add_pipeline(name, pipeline)
            time.sleep(0.001)
            remove_pipeline(name)
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
