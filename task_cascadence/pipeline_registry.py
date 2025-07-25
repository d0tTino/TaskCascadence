"""Registry of running :class:`~task_cascadence.orchestrator.TaskPipeline` instances."""

from __future__ import annotations

from typing import Dict, Optional

from .orchestrator import TaskPipeline

# Running pipelines keyed by task name
_running_pipelines: Dict[str, TaskPipeline] = {}


def add_pipeline(name: str, pipeline: TaskPipeline) -> None:
    """Register *pipeline* under *name*."""
    _running_pipelines[name] = pipeline


def remove_pipeline(name: str) -> None:
    """Remove the pipeline registered as *name* if present."""
    _running_pipelines.pop(name, None)


def get_pipeline(name: str) -> Optional[TaskPipeline]:
    """Return the pipeline registered as *name* if running."""
    return _running_pipelines.get(name)


def list_pipelines() -> Dict[str, TaskPipeline]:
    """Return a copy of the currently registered pipelines."""
    return dict(_running_pipelines)
