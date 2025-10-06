"""Simple in-memory scheduler.

This module provides a minimal scheduler implementation that mimics the
behaviour described in the PRD.  It is intentionally lightweight so the CLI
can interact with tasks without pulling in heavy dependencies like
APScheduler.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
import threading
import re
from uuid import uuid4

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import yaml
from google.protobuf.timestamp_pb2 import Timestamp
import inspect
from ..async_utils import run_coroutine


from typing import Any, Dict, Iterable, Tuple, Optional, TYPE_CHECKING, Coroutine, cast

if TYPE_CHECKING:  # pragma: no cover - used for type hints only
    from ..plugins import BaseTask  # noqa: F401
    from zoneinfo import ZoneInfo


from ..temporal import TemporalBackend
from .. import metrics
from ..http_utils import request_with_retry
from ..ume import _hash_user_id


_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


def _maybe_hash_user_id(user_id: str) -> str:
    """Return ``user_id`` hashed unless it already appears hashed."""

    return user_id if _HASH_RE.match(user_id) else _hash_user_id(user_id)


def _maybe_hash_group_id(group_id: str) -> str:
    """Return ``group_id`` hashed unless it already appears hashed."""

    return group_id if _HASH_RE.match(group_id) else _hash_user_id(group_id)


@dataclass(frozen=True)
class TaskExecutionResult:
    """Wrapper containing metadata for a task execution."""

    run_id: str
    result: Any


class BaseScheduler:
    """Very small task scheduler used by the CLI.

    Parameters
    ----------
    temporal:
        Optional :class:`~task_cascadence.temporal.TemporalBackend` used to
        execute tasks via Temporal.
    """

    def __init__(self, temporal: Optional[TemporalBackend] = None) -> None:
        self._tasks: Dict[str, Dict[str, Any]] = {}
        self._temporal = temporal
        self._schedules_lock = threading.Lock()

    def register_task(
        self,
        name: str,
        task: Any,
        *,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> None:
        """Register a task object under ``name``."""

        with self._schedules_lock:
            self._tasks[name] = {
                "task": task,
                "disabled": False,
                "paused": False,
                "user_id": user_id,
                "group_id": group_id,
            }

    # ------------------------------------------------------------------
    # Query helpers
    def list_tasks(self) -> Iterable[Tuple[str, bool]]:
        """Return an iterable of ``(name, disabled)`` tuples."""

        with self._schedules_lock:
            items = list(self._tasks.items())
        for name, info in items:
            yield name, info["disabled"]

    def is_async(self, name: str) -> bool:
        """Return ``True`` if the task registered under ``name`` is asynchronous."""

        with self._schedules_lock:
            info = self._tasks.get(name)
            if not info:
                raise ValueError(f"Unknown task: {name}")
            task = info["task"]
        run = getattr(task, "run", None)
        return inspect.iscoroutinefunction(run)

    def run_task(
        self,
        name: str,
        *,
        use_temporal: bool | None = None,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> Any:
        """Run a task by name if it exists and is enabled."""

        result = self.run_task_with_metadata(
            name,
            use_temporal=use_temporal,
            user_id=user_id,
            group_id=group_id,
        )
        return result.result

    def run_task_with_metadata(
        self,
        name: str,
        *,
        use_temporal: bool | None = None,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> TaskExecutionResult:
        """Run a task and return its :class:`TaskExecutionResult`."""
        try:  # pragma: no cover - allow tests to omit audit logging
            from ..ume import emit_audit_log
            from ..ume.models import AuditEvent
        except Exception:  # pragma: no cover - fallback when not available
            AuditEvent = None  # type: ignore[assignment]

            def emit_audit_log(*args: object, **kwargs: object) -> None:  # type: ignore[misc]
                return None

        supports_partial = AuditEvent is not None and hasattr(AuditEvent(), "partial")

        with self._schedules_lock:
            info = self._tasks.get(name)
            if not info:
                reason = f"Unknown task: {name}"
                emit_audit_log(
                    name,
                    "run",
                    "error",
                    reason=reason,
                    user_id=user_id,
                    group_id=group_id,
                )
                raise ValueError(reason)
            if info["disabled"]:
                reason = f"Task '{name}' is disabled"
                emit_audit_log(
                    name,
                    "run",
                    "error",
                    reason=reason,
                    user_id=user_id,
                    group_id=group_id,
                )
                raise ValueError(reason)
            if info.get("paused"):
                reason = f"Task '{name}' is paused"
                emit_audit_log(
                    name,
                    "run",
                    "error",
                    reason=reason,
                    user_id=user_id,
                    group_id=group_id,
                )
                raise ValueError(reason)
            if user_id is None:
                reason = "user_id is required"
                emit_audit_log(
                    name,
                    "run",
                    "error",
                    reason=reason,
                    user_id=user_id,
                    group_id=group_id,
                )
                raise ValueError(reason)
            uid = user_id
            task = info["task"]

        run_id = str(uuid4())

        if (use_temporal or (use_temporal is None and self._temporal)):
            if not self._temporal:
                reason = "Temporal backend not configured"
                emit_audit_log(
                    name,
                    "run",
                    "error",
                    reason=reason,
                    user_id=user_id,
                    group_id=group_id,
                    run_id=run_id,
                )
                raise RuntimeError(reason)
            workflow = getattr(task, "workflow", task.__class__.__name__)
            try:
                result = self._temporal.run_workflow_sync(workflow)
            except Exception as exc:
                emit_audit_log(
                    name,
                    "run",
                    "error",
                    reason=str(exc),
                    user_id=user_id,
                    group_id=group_id,
                    run_id=run_id,
                )
                raise
            emit_audit_log(
                name,
                "run",
                "success",
                user_id=user_id,
                group_id=group_id,
                run_id=run_id,
            )
            return TaskExecutionResult(run_id=run_id, result=result)

        if hasattr(task, "run"):
            from datetime import datetime

            from ..ume import emit_task_run
            from ..ume.models import TaskRun, TaskSpec
            from ..orchestrator import TaskPipeline
            from ..pipeline_registry import add_pipeline, remove_pipeline

            spec = TaskSpec(id=task.__class__.__name__, name=task.__class__.__name__)

            @metrics.track_task(name=task.__class__.__name__)
            def runner():
                started = Timestamp()
                started.FromDatetime(datetime.now())
                status = "success"
                result: Any | None = None
                try:
                    if any(
                        hasattr(task, attr)
                        for attr in ("intake", "research", "plan", "verify")
                    ):
                        pipeline = TaskPipeline(task)
                        pipeline.current_run_id = run_id
                        add_pipeline(name, run_id, pipeline)
                        try:
                            result = pipeline.run(user_id=uid, group_id=group_id)
                            if inspect.isawaitable(result):
                                result = run_coroutine(
                                    cast(Coroutine[Any, Any, Any], result)
                                )
                        finally:
                            remove_pipeline(name, run_id)
                            remove_pipeline(run_id)
                    else:
                        task.user_id = uid
                        task.group_id = group_id
                        result = task.run()
                        if inspect.isawaitable(result):
                            result = run_coroutine(
                                cast(Coroutine[Any, Any, Any], result)
                            )
                except Exception as exc:
                    status = "error"
                    partial_data = None if result is None else repr(result)
                    if supports_partial:
                        emit_audit_log(
                            name,
                            "run",
                            "error",
                            reason=str(exc),
                            partial=partial_data,
                            user_id=user_id,
                            group_id=group_id,
                            run_id=run_id,
                        )
                    else:
                        emit_audit_log(
                            name,
                            "run",
                            "error",
                            reason=str(exc),
                            output=partial_data,
                            user_id=user_id,
                            group_id=group_id,
                            run_id=run_id,
                        )
                    raise
                else:
                    emit_audit_log(
                        name,
                        "run",
                        "success",
                        user_id=user_id,
                        group_id=group_id,
                        run_id=run_id,
                    )
                finally:
                    finished = Timestamp()
                    finished.FromDatetime(datetime.now())
                    run = TaskRun(
                        spec=spec,
                        run_id=run_id,
                        status=status,
                        started_at=started,
                        finished_at=finished,
                    )
                    if user_id is None and group_id is None:
                        emit_task_run(run)
                    elif group_id is None:
                        emit_task_run(run, user_id=user_id)
                    elif user_id is None:
                        emit_task_run(run, group_id=group_id)
                    else:
                        emit_task_run(run, user_id=user_id, group_id=group_id)
                return TaskExecutionResult(run_id=run_id, result=result)

            return runner()
        reason = f"Task '{name}' has no run() method"
        emit_audit_log(
            name,
            "run",
            "error",
            reason=reason,
            user_id=user_id,
            group_id=group_id,
        )
        raise AttributeError(reason)

    def replay_history(self, history_path: str) -> None:
        """Replay a workflow history using the configured Temporal backend."""

        if not self._temporal:
            raise RuntimeError("Temporal backend not configured")
        self._temporal.replay(history_path)

    def disable_task(
        self,
        name: str,
        *,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> None:
        """Disable a registered task."""

        with self._schedules_lock:
            if name not in self._tasks:
                raise ValueError(f"Unknown task: {name}")
            self._tasks[name]["disabled"] = True
        from ..ume import emit_audit_log
        emit_audit_log(
            name,
            "scheduler",
            "disabled",
            user_id=user_id,
            group_id=group_id,
        )

    def pause_task(
        self,
        name: str,
        *,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> None:
        """Temporarily pause a registered task."""

        with self._schedules_lock:
            if name not in self._tasks:
                raise ValueError(f"Unknown task: {name}")
            self._tasks[name]["paused"] = True
        from ..ume import emit_audit_log
        emit_audit_log(
            name,
            "scheduler",
            "paused",
            user_id=user_id,
            group_id=group_id,
        )

    def resume_task(
        self,
        name: str,
        *,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> None:
        """Resume a previously paused task."""

        with self._schedules_lock:
            if name not in self._tasks:
                raise ValueError(f"Unknown task: {name}")
            self._tasks[name]["paused"] = False
        from ..ume import emit_audit_log
        emit_audit_log(
            name,
            "scheduler",
            "resumed",
            user_id=user_id,
            group_id=group_id,
        )


class TemporalScheduler(BaseScheduler):
    """Scheduler executing all tasks via :class:`TemporalBackend`."""

    def __init__(self, backend: TemporalBackend | None = None) -> None:
        super().__init__(temporal=backend or TemporalBackend())

    def run_task(
        self,
        name: str,
        *,
        use_temporal: bool | None = None,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> Any:
        return super().run_task(
            name, use_temporal=True, user_id=user_id, group_id=group_id
        )

    def run_task_with_metadata(
        self,
        name: str,
        *,
        use_temporal: bool | None = None,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> TaskExecutionResult:
        return super().run_task_with_metadata(
            name, use_temporal=True, user_id=user_id, group_id=group_id
        )




class CronScheduler(BaseScheduler):
    """APScheduler-based scheduler using cron triggers.

    Provides timezone-aware scheduling of tasks and persists the cron
    expressions to disk so they survive process restarts.
    """

    def __init__(
        self,
        timezone: str | ZoneInfo = "UTC",
        storage_path: str = "schedules.yml",
        tasks: Optional[Dict[str, Any]] = None,
        temporal: Optional[TemporalBackend] = None,


    ):
        super().__init__(temporal=temporal)

        from zoneinfo import ZoneInfo

        self._CronTrigger = CronTrigger
        self._yaml = yaml
        tz = ZoneInfo(timezone) if isinstance(timezone, str) else timezone
        self.scheduler = BackgroundScheduler(timezone=tz)
        self.storage_path = Path(storage_path)
        self.storage_path.parent.mkdir(parents=True, exist_ok=True)
        self.context_path = self.storage_path.with_suffix(
            f"{self.storage_path.suffix}.ctx"
        )
        with self._schedules_lock:
            self.schedules = self._load_schedules()
            self._schedule_context = self._load_schedule_context()
        self._restore_jobs(tasks or {})

    def _load_schedules(self):
        if self.storage_path.exists():
            with open(self.storage_path, "r") as fh:
                data = self._yaml.safe_load(fh) or {}
                if isinstance(data, dict):
                    modified = False
                    for info in data.values():
                        if isinstance(info, dict):
                            if "user_id" in info:
                                uid = info.pop("user_id")
                                info["user_hash"] = _maybe_hash_user_id(uid)
                                modified = True
                            elif "user_hash" in info:
                                uid = info["user_hash"]
                                if uid and not _HASH_RE.match(uid):
                                    info["user_hash"] = _maybe_hash_user_id(uid)
                                    modified = True
                            if "group_id" in info:
                                gid = info.pop("group_id")
                                info["group_hash"] = _maybe_hash_group_id(gid)
                                modified = True
                            elif "group_hash" in info:
                                gid = info["group_hash"]
                                if gid and not _HASH_RE.match(gid):
                                    info["group_hash"] = _maybe_hash_group_id(gid)
                                    modified = True
                    if modified:
                        with open(self.storage_path, "w") as out:
                            self._yaml.safe_dump(data, out)
                    return data
        return {}

    def _load_schedule_context(self) -> dict[str, dict[str, str | None]]:
        if self.context_path.exists():
            with open(self.context_path, "r") as fh:
                data = self._yaml.safe_load(fh) or {}
                if isinstance(data, dict):
                    result: dict[str, dict[str, str | None]] = {}
                    for key, value in data.items():
                        if not isinstance(value, dict):
                            continue
                        context: dict[str, str | None] = {}
                        user_id = value.get("user_id")
                        group_id = value.get("group_id")
                        if isinstance(user_id, str):
                            context["user_id"] = user_id
                        if isinstance(group_id, str):
                            context["group_id"] = group_id
                        if context:
                            result[key] = context
                    return result
        return {}

    def _save_schedule_context(self) -> None:
        if not self.context_path.parent.exists():
            self.context_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.context_path, "w") as fh:
            self._yaml.safe_dump(self._schedule_context, fh)
        try:
            os.chmod(self.context_path, 0o600)
        except OSError:  # pragma: no cover - platform dependent
            pass

    def _restore_jobs(self, tasks):
        with self._schedules_lock:
            items = list(self.schedules.items())
        for job_id, data in items:
            task = tasks.get(job_id)
            if not task:
                continue
            if isinstance(data, dict):
                expr = data.get("expr")
                user_hash = data.get("user_hash") or data.get("user_id")
                group_hash = data.get("group_hash") or data.get("group_id")
            else:
                expr = data
                user_hash = None
                group_hash = None
            context = self._schedule_context.get(job_id, {})
            user_id = context.get("user_id") if context else None
            group_id = context.get("group_id") if context else None
            if user_id is None and user_hash is not None and not _HASH_RE.match(user_hash):
                user_id = user_hash
            if group_id is None and group_hash is not None and not _HASH_RE.match(group_hash):
                group_id = group_hash
            super().register_task(
                job_id, task, user_id=user_id, group_id=group_id
            )
            trigger = self._CronTrigger.from_crontab(
                expr, timezone=self.scheduler.timezone
            )
            self.scheduler.add_job(
                self._wrap_task(task, user_id=user_id, group_id=group_id),
                trigger=trigger,
                id=job_id,
                replace_existing=True,
            )

    def _save_schedules(self):
        with open(self.storage_path, "w") as fh:
            self._yaml.safe_dump(self.schedules, fh)

    # ------------------------------------------------------------------
    # Calendar event integration

    def _fetch_calendar_event(
        self,
        node: str,
        *,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> dict[str, Any]:
        """Retrieve calendar event metadata from UME.

        The default implementation performs an HTTP ``GET`` against the
        ``UME_BASE_URL`` environment variable. ``user_id`` and ``group_id``
        are forwarded as query parameters so the service can apply
        authorisation or filtering.  Tests and consumers may monkeypatch this
        method for custom behaviour.
        """

        base = os.environ.get("UME_BASE_URL")
        if not base:
            raise RuntimeError("UME_BASE_URL not configured")
        node = node.lstrip("/")
        url = f"{base.rstrip('/')}/v1/calendar/nodes/{node}"
        if user_id is not None:
            user_id = _maybe_hash_user_id(user_id)
        if group_id is not None:
            group_id = _maybe_hash_group_id(group_id)
        params: dict[str, str] = {}
        if user_id is not None:
            params["user_id"] = _maybe_hash_user_id(user_id)
        if group_id is not None:
            params["group_id"] = _maybe_hash_group_id(group_id)
        response = request_with_retry("GET", url, timeout=5, params=params or None)
        return response.json()

    def poll_calendar_event(
        self,
        task: Any,
        node: str,
        *,
        interval: int = 300,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> None:
        """Poll a calendar ``node`` and schedule ``task`` on changes.

        The event is fetched immediately and then every ``interval`` seconds to
        refresh the schedule if the recurrence changes.
        """

        def _poll() -> None:
            event = self._fetch_calendar_event(
                node, user_id=user_id, group_id=group_id
            )
            self.schedule_from_event(
                task, event, user_id=user_id, group_id=group_id
            )
            job_id = task.__class__.__name__
            with self._schedules_lock:
                sched_entry = self.schedules.get(job_id)
                if isinstance(sched_entry, dict):
                    sched_entry["calendar_event"] = {"node": node, "poll": interval}
                    self._save_schedules()

        self.scheduler.add_job(
            _poll,
            "interval",
            seconds=interval,
            id=f"poll:{task.__class__.__name__}:{node}",
            replace_existing=True,
        )
        _poll()

    def _wrap_task(
        self, task, user_id: str | None = None, group_id: str | None = None
    ):
        def runner():
            with self._schedules_lock:
                info = self._tasks.get(task.__class__.__name__)
                paused = bool(info and info.get("paused"))
                resolved_user = user_id
                resolved_group = group_id
                if info is not None:
                    if resolved_user is None:
                        resolved_user = info.get("user_id")
                    if resolved_group is None:
                        resolved_group = info.get("group_id")
            if paused:
                return
            self.run_task(
                task.__class__.__name__,
                user_id=resolved_user,
                group_id=resolved_group,
            )

        return runner

    def register_task(
        self,
        name_or_task: str | BaseTask,
        task_or_expr: BaseTask | str,
        *,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> None:
        """Register a task with optional scheduling.

        Parameters
        ----------
        name_or_task:
            Either a task name or the task instance.
        task_or_expr:
            Either the task instance (if ``name_or_task`` is a name) or a cron
            expression used to schedule the task.

        This method supports two calling styles for backwards compatibility
        with :class:`BaseScheduler`:

        ``register_task(name, task)``
            Register ``task`` under ``name`` without scheduling.

        ``register_task(task, cron_expression)``
            Register ``task`` and schedule it using ``cron_expression``.
        """

        try:  # pragma: no cover - ensure audit logging is optional for tests
            from ..ume import emit_audit_log
        except Exception:  # pragma: no cover - fallback when not available
            from typing import Any  # noqa: F401

            def emit_audit_log(*args: Any, **kwargs: Any) -> None:  # type: ignore[misc]
                return None

        user_hash = _maybe_hash_user_id(user_id) if user_id is not None else None
        group_hash = _maybe_hash_group_id(group_id) if group_id is not None else None

        if isinstance(name_or_task, str):
            # Called with ``name`` and ``task``
            name, task = name_or_task, task_or_expr
            try:
                super().register_task(
                    name, task, user_id=user_id, group_id=group_id
                )
            except Exception as exc:  # pragma: no cover - passthrough
                emit_audit_log(
                    name,
                    "register",
                    "error",
                    reason=str(exc),
                    user_id=user_id,
                    group_id=group_id,
                )
                raise
            emit_audit_log(
                name, "register", "success", user_id=user_id, group_id=group_id
            )
            return

        task, cron_expression = name_or_task, task_or_expr
        job_id = task.__class__.__name__
        try:
            super().register_task(
                job_id, task, user_id=user_id, group_id=group_id
            )
            with self._schedules_lock:
                if user_id is None and group_id is None:
                    self.schedules[job_id] = cron_expression
                else:
                    entry: dict[str, Any] = {"expr": cron_expression}
                    if user_hash is not None:
                        entry["user_hash"] = user_hash
                    if group_hash is not None:
                        entry["group_hash"] = group_hash
                    self.schedules[job_id] = entry
                if user_id is not None or group_id is not None:
                    context: dict[str, str | None] = {}
                    if user_id is not None:
                        context["user_id"] = user_id
                    if group_id is not None:
                        context["group_id"] = group_id
                    self._schedule_context[job_id] = context
                elif job_id in self._schedule_context:
                    self._schedule_context.pop(job_id)
                self._save_schedules()
                self._save_schedule_context()

            trigger = self._CronTrigger.from_crontab(
                cron_expression, timezone=self.scheduler.timezone
            )
            self.scheduler.add_job(
                self._wrap_task(task, user_id=user_id, group_id=group_id),
                trigger=trigger,
                id=job_id,
                replace_existing=True,
            )
        except Exception as exc:  # pragma: no cover - passthrough
            emit_audit_log(
                job_id,
                "schedule",
                "error",
                reason=str(exc),
                user_id=user_id,
                group_id=group_id,
            )
            raise
        emit_audit_log(
            job_id, "schedule", "success", user_id=user_id, group_id=group_id
        )

    def schedule_task(
        self,
        task: Any,
        cron_expression: str,
        *,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> None:
        """Convenience wrapper for :meth:`register_task`."""
        self.register_task(
            name_or_task=task,
            task_or_expr=cron_expression,
            user_id=user_id,
            group_id=group_id,
        )

    def start(self):
        self.scheduler.start()

    def shutdown(self, wait=True):
        self.scheduler.shutdown(wait=wait)

    def list_jobs(self):
        return self.scheduler.get_jobs()

    def load_yaml(self, path: str | Path, tasks: dict[str, Any] | None = None) -> None:
        """Load cron schedules from ``path`` and register them.

        The YAML file should map job identifiers to either a cron expression
        string, a dictionary with an ``expr`` key or an entry containing a
        ``calendar_event`` reference. Optional ``user_id``/``group_id`` and
        ``recurrence`` metadata may be supplied. ``tasks`` provides a mapping
        of job identifiers to task instances.  When omitted the scheduler's
        already-registered tasks are used.
        """

        data = self._yaml.safe_load(Path(path).read_text()) or {}
        for job_id, entry in data.items():
            if isinstance(entry, str):
                info: dict[str, Any] = {"expr": entry}
            elif isinstance(entry, dict):
                info = dict(entry)
            else:
                raise ValueError(f"{job_id}: entry must be a string or mapping")

            task_obj = (tasks or {}).get(job_id)
            if not task_obj:
                with self._schedules_lock:
                    if job_id in self._tasks:
                        task_obj = self._tasks[job_id]["task"]
            if not task_obj:
                continue

            has_ce = "calendar_event" in info
            has_expr = "expr" in info
            if not has_ce and not has_expr:
                raise ValueError(f"{job_id}: missing 'expr' or 'calendar_event'")

            if has_ce:
                ce = info["calendar_event"]
                if isinstance(ce, dict):
                    node = ce.get("node")
                    if not node or not isinstance(node, str):
                        raise ValueError(
                            f"{job_id}: calendar_event missing 'node'"
                        )
                    poll = ce.get("poll")
                    user_id = ce.get("user_id", info.get("user_id"))
                    group_id = ce.get("group_id", info.get("group_id"))
                elif isinstance(ce, str):
                    if not ce:
                        raise ValueError(
                            f"{job_id}: calendar_event node is required"
                        )
                    node = ce
                    poll = info.get("poll")
                    user_id = info.get("user_id")
                    group_id = info.get("group_id")
                else:
                    raise ValueError(
                        f"{job_id}: 'calendar_event' must be string or mapping"
                    )
                if poll:
                    self.poll_calendar_event(
                        task_obj, node, interval=poll, user_id=user_id, group_id=group_id
                    )
                else:
                    event = self._fetch_calendar_event(
                        node, user_id=user_id, group_id=group_id
                    )
                    self.schedule_from_event(
                        task_obj, event, user_id=user_id, group_id=group_id
                    )
                    with self._schedules_lock:
                        sched_entry = self.schedules.get(job_id)
                        if isinstance(sched_entry, dict):
                            sched_entry["calendar_event"] = {"node": node}
                            self._save_schedules()
                continue

            expr = info.get("expr")
            if not isinstance(expr, str) or not expr:
                raise ValueError(f"{job_id}: 'expr' must be a non-empty string")
            self.register_task(
                name_or_task=task_obj,
                task_or_expr=expr,
                user_id=info.get("user_id"),
                group_id=info.get("group_id"),
            )

            if "recurrence" in info:
                with self._schedules_lock:
                    sched_entry = self.schedules.get(job_id)
                    if isinstance(sched_entry, dict):
                        sched_entry["recurrence"] = info["recurrence"]
                    else:
                        self.schedules[job_id] = {
                            "expr": expr,
                            "recurrence": info["recurrence"],
                        }
                    self._save_schedules()

        with self._schedules_lock:
            self._save_schedules()

    def schedule_from_event(
        self,
        task: Any,
        event: dict[str, Any],
        *,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> None:
        """Schedule ``task`` according to ``event`` recurrence settings.

        Parameters
        ----------
        task:
            The task instance to schedule.
        event:
            Event payload containing recurrence information.
        user_id, group_id:
            Optional identifiers propagated to :meth:`register_task` and stored
            alongside the schedule metadata.
        """

        try:  # pragma: no cover - allow tests to omit audit logging
            from ..ume import emit_audit_log
        except Exception:  # pragma: no cover - fallback when not available
            from typing import Any  # noqa: F401

            def emit_audit_log(*args: Any, **kwargs: Any) -> None:  # type: ignore[misc]
                return None

        job_id = task.__class__.__name__
        recurrence = event.get("recurrence", {})
        expr = recurrence.get("cron")
        group_hash = _maybe_hash_group_id(group_id) if group_id is not None else None
        if not expr:
            emit_audit_log(
                job_id,
                "schedule",
                "error",
                reason="event missing recurrence cron",
                user_id=user_id,
                group_id=group_id,
            )
            raise ValueError("event missing recurrence cron")

        try:
            self.register_task(
                task,
                expr,
                user_id=user_id,
                group_id=group_id,
            )
            user_hash = _maybe_hash_user_id(user_id) if user_id is not None else None
            with self._schedules_lock:
                sched_entry = self.schedules.get(job_id)
                if isinstance(sched_entry, dict):
                    sched_entry["recurrence"] = recurrence
                    if user_hash is not None:
                        sched_entry["user_hash"] = user_hash
                    if group_hash is not None:
                        sched_entry["group_hash"] = group_hash
                else:
                    entry: dict[str, Any] = {"expr": expr, "recurrence": recurrence}
                    if user_hash is not None:
                        entry["user_hash"] = user_hash
                    if group_hash is not None:
                        entry["group_hash"] = group_hash
                    self.schedules[job_id] = entry
                self._save_schedules()
                self._save_schedule_context()
        except Exception as exc:  # pragma: no cover - passthrough
            emit_audit_log(
                job_id,
                "schedule",
                "error",
                reason=str(exc),
                user_id=user_id,
                group_id=group_id,
            )
            raise
        emit_audit_log(
            job_id, "schedule", "success", user_id=user_id, group_id=group_id
        )

    def unschedule(self, name: str) -> None:
        """Remove the cron schedule for ``name``."""

        try:  # pragma: no cover - allow tests to omit audit logging
            from ..ume import emit_audit_log
        except Exception:  # pragma: no cover - fallback when not available
            from typing import Any  # noqa: F401

            def emit_audit_log(*args: Any, **kwargs: Any) -> None:  # type: ignore[misc]
                return None

        with self._schedules_lock:
            sched_entry = self.schedules.get(name)
            if not sched_entry:
                emit_audit_log(
                    name,
                    "unschedule",
                    "error",
                    reason="unknown schedule",
                )
                raise ValueError(f"Unknown schedule: {name}")
            if isinstance(sched_entry, dict):
                user_hash = sched_entry.get("user_hash") or sched_entry.get("user_id")
                group_hash = sched_entry.get("group_hash") or sched_entry.get("group_id")
            else:
                user_hash = group_hash = None
            context = self._schedule_context.get(name, {})
            plain_user = context.get("user_id") if isinstance(context, dict) else None
            plain_group = context.get("group_id") if isinstance(context, dict) else None
            if isinstance(plain_user, str):
                user_id = plain_user
            else:
                user_id = None
            if isinstance(plain_group, str):
                group_id = plain_group
            else:
                group_id = None
            if user_id is None and user_hash is not None and not _HASH_RE.match(user_hash):
                user_id = user_hash
            if group_id is None and group_hash is not None and not _HASH_RE.match(group_hash):
                group_id = group_hash

            try:
                self.scheduler.remove_job(name)
            except Exception as exc:  # pragma: no cover - passthrough
                emit_audit_log(
                    name,
                    "unschedule",
                    "error",
                    reason=str(exc),
                    user_id=user_id,
                    group_id=group_id,
                )
            self.schedules.pop(name, None)
            self._save_schedules()
            self._schedule_context.pop(name, None)
            self._save_schedule_context()
        # Ensure stage events are persisted even when no transport is configured
        from ..stage_store import StageStore
        StageStore().add_event(name, "unschedule", None, None)

        from ..ume import emit_stage_update_event, emit_audit_log

        emit_stage_update_event(name, "unschedule")
        emit_audit_log(
            name,
            "unschedule",
            "success",
            user_id=user_id,
            group_id=group_id,
        )

    def disable_task(
        self,
        name: str,
        *,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> None:
        """Disable ``name`` and emit a stage event."""

        from ..ume import emit_stage_update_event
        try:  # pragma: no cover - allow tests to omit audit logging
            from ..ume import emit_audit_log
        except Exception:  # pragma: no cover - fallback when not available
            from typing import Any  # noqa: F401

            def emit_audit_log(*args: Any, **kwargs: Any) -> None:  # type: ignore[misc]
                return None

        try:
            super().disable_task(name, user_id=user_id, group_id=group_id)
        except Exception as exc:  # pragma: no cover - passthrough
            emit_audit_log(
                name,
                "disabled",
                "error",
                reason=str(exc),
                user_id=user_id,
                group_id=group_id,
            )
            raise
        emit_stage_update_event(
            name, "disabled", user_id=user_id, group_id=group_id
        )
        emit_audit_log(
            name, "disabled", "success", user_id=user_id, group_id=group_id
        )

    def pause_task(
        self,
        name: str,
        *,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> None:
        """Pause ``name`` and emit a stage event."""

        from ..ume import emit_stage_update_event
        try:  # pragma: no cover - allow tests to omit audit logging
            from ..ume import emit_audit_log
        except Exception:  # pragma: no cover - fallback when not available
            from typing import Any  # noqa: F401

            def emit_audit_log(*args: Any, **kwargs: Any) -> None:  # type: ignore[misc]
                return None

        try:
            super().pause_task(name, user_id=user_id, group_id=group_id)
        except Exception as exc:  # pragma: no cover - passthrough
            emit_audit_log(
                name,
                "paused",
                "error",
                reason=str(exc),
                user_id=user_id,
                group_id=group_id,
            )
            raise
        emit_stage_update_event(
            name, "paused", user_id=user_id, group_id=group_id
        )
        emit_audit_log(
            name, "paused", "success", user_id=user_id, group_id=group_id
        )

    def resume_task(
        self,
        name: str,
        *,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> None:
        """Resume ``name`` and emit a stage event."""

        from ..ume import emit_stage_update_event
        try:  # pragma: no cover - allow tests to omit audit logging
            from ..ume import emit_audit_log
        except Exception:  # pragma: no cover - fallback when not available
            from typing import Any  # noqa: F401

            def emit_audit_log(*args: Any, **kwargs: Any) -> None:  # type: ignore[misc]
                return None

        try:
            super().resume_task(name, user_id=user_id, group_id=group_id)
        except Exception as exc:  # pragma: no cover - passthrough
            emit_audit_log(
                name,
                "resumed",
                "error",
                reason=str(exc),
                user_id=user_id,
                group_id=group_id,
            )
            raise
        emit_stage_update_event(
            name, "resumed", user_id=user_id, group_id=group_id
        )
        emit_audit_log(
            name, "resumed", "success", user_id=user_id, group_id=group_id
        )


# ---------------------------------------------------------------------------
# Default scheduler accessor

_default_scheduler: BaseScheduler | None = None


def set_default_scheduler(scheduler: BaseScheduler) -> None:
    """Set the global default scheduler instance."""

    global _default_scheduler
    _default_scheduler = scheduler


def get_default_scheduler() -> BaseScheduler:
    """Return the configured default scheduler."""

    if _default_scheduler is None:
        raise RuntimeError("Default scheduler has not been initialised")
    return _default_scheduler


# Backwards compatibility alias
default_scheduler = get_default_scheduler


def create_scheduler(
    backend: str,
    tasks: dict[str, Any] | None = None,
    *,
    timezone: str | ZoneInfo = "UTC",
) -> BaseScheduler:
    """Factory returning a scheduler for ``backend``."""

    tasks = tasks or {}
    if backend == "cron":
        return CronScheduler(timezone=timezone, tasks=tasks)
    if backend == "base":
        return BaseScheduler()
    if backend == "temporal":
        return TemporalScheduler()
    if backend == "cronyx":
        from .cronyx import CronyxScheduler
        return CronyxScheduler()
    if backend == "dag":
        from .dag import DagCronScheduler
        return DagCronScheduler(timezone=timezone, tasks=tasks)
    raise ValueError(f"Unknown scheduler backend: {backend}")



