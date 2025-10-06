"""Registry of running :class:`~task_cascadence.orchestrator.TaskPipeline` instances."""

from __future__ import annotations

from threading import Lock
from typing import Any, Dict, Optional

from .orchestrator import TaskPipeline

# Running pipelines keyed by task name and run identifier
_running_pipelines: Dict[str, Dict[str, TaskPipeline]] = {}
# Running pipelines keyed by run/job identifier
_running_pipelines: Dict[str, TaskPipeline] = {}
# Mapping of run/job identifier back to originating task name
_pipeline_tasks: Dict[str, str] = {}
# Mapping of task name to a list of associated run identifiers (oldest -> newest)
_task_run_index: Dict[str, list[str]] = {}
# Lock protecting access to the running pipeline registry
_registry_lock = Lock()


def add_pipeline(name: str, run_id: str, pipeline: TaskPipeline) -> None:
    """Register *pipeline* under *name* and *run_id*."""

    with _registry_lock:
        pipelines = _running_pipelines.setdefault(name, {})
        pipelines[run_id] = pipeline

def add_pipeline(task_name: str, run_id: str, pipeline: TaskPipeline) -> None:
    """Register *pipeline* for *task_name* under the unique *run_id*."""

    with _registry_lock:
        _running_pipelines[run_id] = pipeline
        _pipeline_tasks[run_id] = task_name
        runs_for_task = _task_run_index.setdefault(task_name, [])
        runs_for_task.append(run_id)

def remove_pipeline(name: str, run_id: str) -> None:
    """Remove the pipeline registered under *name*/*run_id* if present."""

    with _registry_lock:
        pipelines = _running_pipelines.get(name)
        if not pipelines:
            return
        pipelines.pop(run_id, None)
        if not pipelines:
            _running_pipelines.pop(name, None)
def remove_pipeline(run_id: str) -> None:
    """Remove the pipeline registered under *run_id* if present."""

    with _registry_lock:
        task_name = _pipeline_tasks.pop(run_id, None)
        _running_pipelines.pop(run_id, None)
        if task_name is not None:
            runs_for_task = _task_run_index.get(task_name)
            if runs_for_task is not None:
                try:
                    runs_for_task.remove(run_id)
                except ValueError:  # pragma: no cover - defensive cleanup
                    pass
                if not runs_for_task:
                    _task_run_index.pop(task_name, None)


def get_pipeline(run_id: str) -> Optional[TaskPipeline]:
    """Return the pipeline registered under *run_id* if running."""

    with _registry_lock:
        return _running_pipelines.get(run_id)

def get_pipeline(name: str, run_id: Optional[str] = None) -> Optional[TaskPipeline]:
    """Return the pipeline registered as *name* (optionally filtered by *run_id*)."""

def get_latest_pipeline_for_task(task_name: str) -> Optional[TaskPipeline]:
    """Return the most recently registered pipeline for *task_name* if running."""

    with _registry_lock:
        run_ids = _task_run_index.get(task_name)
        if not run_ids:
            return None
        # The list is maintained oldest -> newest, so walk backwards to find the
        # first run id that is still registered. Defensive cleanup ensures stale
        # identifiers are removed if encountered.
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

    with _registry_lock:
        pipelines = _running_pipelines.get(name)
        if not pipelines:
            return None
        if run_id is not None:
            return pipelines.get(run_id)
        if len(pipelines) == 1:
            return next(iter(pipelines.values()))
        return None


def list_pipelines() -> Dict[str, Dict[str, TaskPipeline]]:
    """Return a copy of the currently registered pipelines."""
def list_pipelines() -> Dict[str, TaskPipeline]:
    """Return a copy of the currently registered pipelines keyed by run id."""

    with _registry_lock:
        return {name: dict(pipes) for name, pipes in _running_pipelines.items()}


def attach_pipeline_context(
    run_id_or_task: str,
    context: Any,
    *,
    user_id: str | None = None,
    group_id: str | None = None,
    run_id: str | None = None,
) -> bool:
    """Attach *context* to the running pipeline identified by *run_id_or_task*.

    The identifier is treated as a run/job identifier first. For backward
    compatibility the lookup falls back to the latest pipeline registered for
    the task of the same name when no pipeline exists for the identifier.
    Returns ``True`` when a pipeline was found and context enqueued.
    """

    pipeline = get_pipeline(name, run_id=run_id)
    pipeline = get_pipeline(run_id_or_task)
    if pipeline is None:
        pipeline = get_latest_pipeline_for_task(run_id_or_task)
        if pipeline is None:
            return False
    pipeline.attach_context(context, user_id=user_id, group_id=group_id)
    return True
