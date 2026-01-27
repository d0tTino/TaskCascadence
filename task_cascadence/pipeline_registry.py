"""Registry of running :class:`~task_cascadence.orchestrator.TaskPipeline` instances."""

from __future__ import annotations

from threading import Lock
from typing import Any, Dict, Optional

from .orchestrator import TaskPipeline

# Running pipelines keyed by run/job identifier
_running_pipelines: Dict[str, TaskPipeline] = {}
# Mapping of run/job identifier back to originating task name
_pipeline_tasks: Dict[str, str] = {}
# Mapping of task name to a list of associated run identifiers (oldest -> newest)
_task_run_index: Dict[str, list[str]] = {}
# Lock protecting access to the running pipeline registry
_registry_lock = Lock()


def add_pipeline(task_name: str, run_id: str, pipeline: TaskPipeline) -> None:
    """Register *pipeline* for *task_name* under the unique *run_id*."""

    with _registry_lock:
        _running_pipelines[run_id] = pipeline
        _pipeline_tasks[run_id] = task_name
        _task_run_index.setdefault(task_name, []).append(run_id)


def remove_pipeline(run_id: str, task_name: str | None = None) -> None:
    """Remove the pipeline registered under *run_id* if present."""

    with _registry_lock:
        _running_pipelines.pop(run_id, None)
        resolved_task = task_name or _pipeline_tasks.pop(run_id, None)
        if resolved_task is None:
            return
        runs_for_task = _task_run_index.get(resolved_task)
        if not runs_for_task:
            return
        try:
            runs_for_task.remove(run_id)
        except ValueError:  # pragma: no cover - defensive cleanup
            pass
        if not runs_for_task:
            _task_run_index.pop(resolved_task, None)


def _clean_latest_for_task(task_name: str) -> Optional[TaskPipeline]:
    """Return the latest pipeline for *task_name* while pruning stale entries."""

    run_ids = _task_run_index.get(task_name)
    if not run_ids:
        return None

    for index in range(len(run_ids) - 1, -1, -1):
        run_id = run_ids[index]
        pipeline = _running_pipelines.get(run_id)
        if pipeline is not None:
            return pipeline
        # Stale identifier encountered; remove it before continuing.
        del run_ids[index]
    if not run_ids:
        _task_run_index.pop(task_name, None)
    return None


def get_pipeline(run_id: str) -> Optional[TaskPipeline]:
    """Return the pipeline registered under the exact *run_id*."""

    with _registry_lock:
        return _running_pipelines.get(run_id)


def get_latest_pipeline_for_task(task_name: str) -> Optional[TaskPipeline]:
    """Return the most recently registered pipeline for *task_name* if running."""

    with _registry_lock:
        return _clean_latest_for_task(task_name)


def list_pipelines() -> Dict[str, TaskPipeline]:
    """Return a copy of the currently registered pipelines keyed by run id."""

    with _registry_lock:
        return dict(_running_pipelines)


def attach_pipeline_context(
    run_id: str,
    context: Any,
    *,
    user_id: str | None = None,
    group_id: str | None = None,
) -> bool:
    """Attach *context* to the running pipeline identified by *run_id*.

    Returns ``True`` when a pipeline was found and context enqueued.
    """

    pipeline = get_pipeline(run_id)
    if pipeline is None:
        return False
    pipeline.attach_context(context, user_id=user_id, group_id=group_id)
    return True


__all__ = [
    "add_pipeline",
    "remove_pipeline",
    "get_pipeline",
    "get_latest_pipeline_for_task",
    "list_pipelines",
    "attach_pipeline_context",
]
