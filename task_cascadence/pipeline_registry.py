"""Registry of running :class:`~task_cascadence.orchestrator.TaskPipeline` instances."""

from __future__ import annotations

from typing import Any, Dict, Optional
from threading import Lock

from .orchestrator import TaskPipeline

# Running pipelines keyed by task name and run identifier
_running_pipelines: Dict[str, Dict[str, TaskPipeline]] = {}
# Lock protecting access to the running pipeline registry
_registry_lock = Lock()


def add_pipeline(name: str, run_id: str, pipeline: TaskPipeline) -> None:
    """Register *pipeline* under *name* and *run_id*."""

    with _registry_lock:
        pipelines = _running_pipelines.setdefault(name, {})
        pipelines[run_id] = pipeline


def remove_pipeline(name: str, run_id: str) -> None:
    """Remove the pipeline registered under *name*/*run_id* if present."""

    with _registry_lock:
        pipelines = _running_pipelines.get(name)
        if not pipelines:
            return
        pipelines.pop(run_id, None)
        if not pipelines:
            _running_pipelines.pop(name, None)


def get_pipeline(name: str, run_id: Optional[str] = None) -> Optional[TaskPipeline]:
    """Return the pipeline registered as *name* (optionally filtered by *run_id*)."""

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

    with _registry_lock:
        return {name: dict(pipes) for name, pipes in _running_pipelines.items()}


def attach_pipeline_context(
    name: str,
    context: Any,
    *,
    user_id: str | None = None,
    group_id: str | None = None,
    run_id: str | None = None,
) -> bool:
    """Attach *context* to the running pipeline registered as *name*.

    Returns ``True`` if the pipeline was found and context enqueued.
    """

    pipeline = get_pipeline(name, run_id=run_id)
    if pipeline is None:
        return False
    pipeline.attach_context(context, user_id=user_id, group_id=group_id)
    return True
