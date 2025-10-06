"""Registry of active :class:`~task_cascadence.orchestrator.TaskPipeline` instances."""

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


def _get_latest_pipeline_for_task_locked(task_name: str) -> Optional[TaskPipeline]:
    """Return the most recent pipeline for *task_name* while holding the lock."""

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


def add_pipeline(task_name: str, run_id: str, pipeline: TaskPipeline) -> None:
    """Register *pipeline* for *task_name* under the unique *run_id*."""

    with _registry_lock:
        _running_pipelines[run_id] = pipeline
        _pipeline_tasks[run_id] = task_name
        _task_run_index.setdefault(task_name, []).append(run_id)


def remove_pipeline(identifier: str, run_id: Optional[str] = None) -> None:
    """Remove the pipeline identified by *run_id* (or legacy *identifier*).*"""

    with _registry_lock:
        run_id_key = run_id if run_id is not None else identifier
        _running_pipelines.pop(run_id_key, None)

        mapped_task = _pipeline_tasks.pop(run_id_key, None)
        if mapped_task is None and run_id is not None:
            mapped_task = identifier

        if mapped_task:
            runs_for_task = _task_run_index.get(mapped_task)
            if runs_for_task:
                try:
                    runs_for_task.remove(run_id_key)
                except ValueError:  # pragma: no cover - defensive cleanup
                    pass
                if not runs_for_task:
                    _task_run_index.pop(mapped_task, None)


def get_pipeline(identifier: str, run_id: Optional[str] = None) -> Optional[TaskPipeline]:
    """Return the pipeline registered under *identifier* or *run_id* if running."""

    with _registry_lock:
        if run_id is not None:
            pipeline = _running_pipelines.get(run_id)
            if pipeline is None:
                return None
            mapped_task = _pipeline_tasks.get(run_id)
            if mapped_task is None or mapped_task == identifier:
                return pipeline
            return None

        pipeline = _running_pipelines.get(identifier)
        if pipeline is not None:
            return pipeline
        return _get_latest_pipeline_for_task_locked(identifier)

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


def get_pipeline(identifier: str, *, run_id: str | None = None) -> Optional[TaskPipeline]:
    """Return the pipeline registered as *identifier*.

    ``identifier`` is treated as a run identifier first. For backward
    compatibility, when no pipeline exists for the identifier it falls back to
    the most recent pipeline registered for the task of the same name. A
    specific *run_id* can be provided to bypass the lookup heuristic.
    """

    with _registry_lock:
        return _get_latest_pipeline_for_task_locked(task_name)
        if run_id is not None:
            pipeline = _running_pipelines.get(run_id)
            if pipeline is not None:
                return pipeline

        pipeline = _running_pipelines.get(identifier)
        if pipeline is not None:
            return pipeline

        return _clean_latest_for_task(identifier)


def get_latest_pipeline_for_task(task_name: str) -> Optional[TaskPipeline]:
    """Return the most recently registered pipeline for *task_name* if running."""

    with _registry_lock:
        return _clean_latest_for_task(task_name)


def list_pipelines() -> Dict[str, TaskPipeline]:
    """Return a copy of the currently registered pipelines keyed by run id."""

    with _registry_lock:
        return dict(_running_pipelines)


def attach_pipeline_context(
    run_id_or_task: str,
    context: Any,
    *,
    user_id: str | None = None,
    group_id: str | None = None,
) -> bool:
    """Attach *context* to the running pipeline identified by *run_id_or_task*.

    The identifier is treated as a run/job identifier first. For backward
    compatibility the lookup falls back to the latest pipeline registered for
    the task of the same name when no pipeline exists for the identifier.
    Returns ``True`` when a pipeline was found and context enqueued.
    """

    with _registry_lock:
        if run_id is not None:
            pipeline = _running_pipelines.get(run_id)
        else:
            pipeline = _running_pipelines.get(run_id_or_task)
            if pipeline is None:
                pipeline = _get_latest_pipeline_for_task_locked(run_id_or_task)

    if pipeline is None:
        return False

    pipeline = get_pipeline(run_id_or_task)
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
