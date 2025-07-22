from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class TaskSpec:
    """Definition of a task to be executed."""

    id: str
    name: str
    description: Optional[str] = None
    user_hash: Optional[str] = None


@dataclass
class TaskRun:
    """Metadata about a specific execution of a task."""

    spec: TaskSpec
    run_id: str
    status: str
    started_at: datetime
    finished_at: datetime
    user_hash: Optional[str] = None


@dataclass
class TaskPointer:
    """Reference to another user's task run."""

    run_id: str
    user_hash: str


@dataclass
class PointerUpdate:
    """Pointer synchronization message."""

    task_name: str
    run_id: str
    user_hash: str
