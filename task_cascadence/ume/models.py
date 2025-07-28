from __future__ import annotations

from .protos import tasks_pb2


TaskSpec = tasks_pb2.TaskSpec  # type: ignore[attr-defined]
"""Definition of a task to be executed."""


TaskRun = tasks_pb2.TaskRun  # type: ignore[attr-defined]
"""Metadata about a specific execution of a task."""


TaskPointer = tasks_pb2.TaskPointer  # type: ignore[attr-defined]
"""Reference to another user's task run."""


PointerUpdate = tasks_pb2.PointerUpdate  # type: ignore[attr-defined]
"""Pointer synchronization message."""


TaskNote = tasks_pb2.TaskNote  # type: ignore[attr-defined]
"""Generic task note or outcome message."""


IdeaSeed = tasks_pb2.IdeaSeed  # type: ignore[attr-defined]
"""Freeform idea prompt from a user."""

__all__ = [
    "TaskSpec",
    "TaskRun",
    "TaskPointer",
    "PointerUpdate",
    "TaskNote",
    "IdeaSeed",
]
