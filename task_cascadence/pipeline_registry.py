"""Registry of running :class:`~task_cascadence.orchestrator.TaskPipeline` instances."""

from __future__ import annotations

from typing import Dict, Optional
from threading import Lock

from .orchestrator import TaskPipeline

# Running pipelines keyed by task name
_running_pipelines: Dict[str, TaskPipeline] = {}
# Lock protecting access to the running pipeline registry
_registry_lock = Lock()


def add_pipeline(name: str, pipeline: TaskPipeline) -> None:
    """Register *pipeline* under *name*."""
    with _registry_lock:
        _running_pipelines[name] = pipeline


def remove_pipeline(name: str) -> None:
    """Remove the pipeline registered as *name* if present."""
    with _registry_lock:
        _running_pipelines.pop(name, None)


def get_pipeline(name: str) -> Optional[TaskPipeline]:
    """Return the pipeline registered as *name* if running."""
    return _running_pipelines.get(name)


def list_pipelines() -> Dict[str, TaskPipeline]:
    """Return a copy of the currently registered pipelines."""
    with _registry_lock:
        return dict(_running_pipelines)
