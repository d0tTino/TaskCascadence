from __future__ import annotations

from .protos import tasks_pb2


TaskSpec = tasks_pb2.TaskSpec
"""Definition of a task to be executed."""


TaskRun = tasks_pb2.TaskRun
"""Metadata about a specific execution of a task."""


TaskPointer = tasks_pb2.TaskPointer
"""Reference to another user's task run."""


PointerUpdate = tasks_pb2.PointerUpdate
"""Pointer synchronization message."""

__all__ = ["TaskSpec", "TaskRun", "TaskPointer", "PointerUpdate"]
