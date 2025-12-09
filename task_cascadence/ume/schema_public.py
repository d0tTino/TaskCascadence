"""Public, versioned schemas for UME events."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from typing import Any, Optional, cast

from google.protobuf.timestamp_pb2 import Timestamp

from . import models

SCHEMA_VERSION = "2024.09.0"


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _from_timestamp(ts: Optional[Timestamp]) -> Optional[datetime]:
    if ts is None or (not ts.seconds and not ts.nanos):
        return None
    return ts.ToDatetime().astimezone(timezone.utc)


def _to_timestamp(dt: Optional[datetime]) -> Timestamp:
    ts = Timestamp()
    if dt is None:
        return ts
    ts.FromDatetime(_ensure_utc(dt))
    return ts


def _convert_value(value: Any) -> Any:
    if is_dataclass(value):
        return cast(BaseSchema, value).to_dict()
    if isinstance(value, datetime):
        return _ensure_utc(value).isoformat()
    return value


@dataclass
class BaseSchema:
    """Base dataclass for public schemas that embeds version metadata."""

    schema_version: str = field(default=SCHEMA_VERSION, init=False)

    def to_dict(self) -> dict[str, Any]:
        return {k: _convert_value(v) for k, v in asdict(self).items()}


@dataclass
class TaskSpecSchema(BaseSchema):
    id: str
    name: str
    description: str | None = None
    user_hash: str | None = None
    user_id: str | None = None
    group_id: str | None = None

    @classmethod
    def from_proto(cls, message: models.TaskSpec) -> "TaskSpecSchema":
        return cls(
            id=message.id,
            name=message.name,
            description=message.description or None,
            user_hash=message.user_hash or None,
            user_id=message.user_id or None,
            group_id=message.group_id or None,
        )

    def to_proto(self) -> models.TaskSpec:
        message = models.TaskSpec(
            id=self.id,
            name=self.name,
            description=self.description or "",
        )
        if self.user_hash is not None:
            message.user_hash = self.user_hash
        if self.user_id is not None:
            message.user_id = self.user_id
        if self.group_id is not None:
            message.group_id = self.group_id
        return message


@dataclass
class TaskRunSchema(BaseSchema):
    spec: TaskSpecSchema
    run_id: str
    status: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    user_hash: str | None = None
    user_id: str | None = None
    group_id: str | None = None

    @classmethod
    def from_proto(cls, message: models.TaskRun) -> "TaskRunSchema":
        return cls(
            spec=TaskSpecSchema.from_proto(message.spec),
            run_id=message.run_id,
            status=message.status,
            started_at=_from_timestamp(message.started_at),
            finished_at=_from_timestamp(message.finished_at),
            user_hash=message.user_hash or None,
            user_id=message.user_id or None,
            group_id=message.group_id or None,
        )

    def to_proto(self) -> models.TaskRun:
        message = models.TaskRun(
            spec=self.spec.to_proto(),
            run_id=self.run_id,
            status=self.status,
            started_at=_to_timestamp(self.started_at),
            finished_at=_to_timestamp(self.finished_at),
        )
        if self.user_hash is not None:
            message.user_hash = self.user_hash
        if self.user_id is not None:
            message.user_id = self.user_id
        if self.group_id is not None:
            message.group_id = self.group_id
        return message


@dataclass
class StageUpdateSchema(BaseSchema):
    task_name: str
    stage: str
    user_hash: str | None = None
    user_id: str | None = None
    group_id: str | None = None

    @classmethod
    def from_proto(cls, message: models.StageUpdate) -> "StageUpdateSchema":
        return cls(
            task_name=message.task_name,
            stage=message.stage,
            user_hash=message.user_hash or None,
            user_id=message.user_id or None,
            group_id=message.group_id or None,
        )

    def to_proto(self) -> models.StageUpdate:
        message = models.StageUpdate(task_name=self.task_name, stage=self.stage)
        if self.user_hash is not None:
            message.user_hash = self.user_hash
        if self.user_id is not None:
            message.user_id = self.user_id
        if self.group_id is not None:
            message.group_id = self.group_id
        return message


@dataclass
class AuditEventSchema(BaseSchema):
    task_name: str
    stage: str
    status: str
    reason: str | None = None
    output: str | None = None
    partial: Any | None = None
    user_hash: str | None = None
    user_id: str | None = None
    group_id: str | None = None

    @classmethod
    def from_proto(cls, message: models.AuditEvent) -> "AuditEventSchema":
        return cls(
            task_name=message.task_name,
            stage=message.stage,
            status=message.status,
            reason=getattr(message, "reason", None) or None,
            output=getattr(message, "output", None) or None,
            partial=getattr(message, "partial", None) or None,
            user_hash=message.user_hash or None,
            user_id=message.user_id or None,
            group_id=message.group_id or None,
        )

    def to_proto(self) -> models.AuditEvent:
        message = models.AuditEvent(
            task_name=self.task_name,
            stage=self.stage,
            status=self.status,
        )
        if self.reason is not None:
            message.reason = self.reason
        if self.output is not None:
            message.output = self.output
        if self.user_hash is not None:
            message.user_hash = self.user_hash
        if self.user_id is not None:
            message.user_id = self.user_id
        if self.group_id is not None:
            message.group_id = self.group_id
        return message


__all__ = [
    "AuditEventSchema",
    "BaseSchema",
    "SCHEMA_VERSION",
    "StageUpdateSchema",
    "TaskRunSchema",
    "TaskSpecSchema",
]
