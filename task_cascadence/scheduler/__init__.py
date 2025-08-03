"""Simple in-memory scheduler.

This module provides a minimal scheduler implementation that mimics the
behaviour described in the PRD.  It is intentionally lightweight so the CLI
can interact with tasks without pulling in heavy dependencies like
APScheduler.
"""

from __future__ import annotations

from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import yaml
from google.protobuf.timestamp_pb2 import Timestamp
import asyncio
import inspect


from typing import Any, Dict, Iterable, Tuple, Optional, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - used for type hints only
    from ..plugins import BaseTask  # noqa: F401
    from zoneinfo import ZoneInfo


from ..temporal import TemporalBackend
from .. import metrics


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

    def register_task(self, name: str, task: Any) -> None:
        """Register a task object under ``name``."""

        self._tasks[name] = {"task": task, "disabled": False, "paused": False}

    # ------------------------------------------------------------------
    # Query helpers
    def list_tasks(self) -> Iterable[Tuple[str, bool]]:
        """Return an iterable of ``(name, disabled)`` tuples."""

        for name, info in self._tasks.items():
            yield name, info["disabled"]

    def is_async(self, name: str) -> bool:
        """Return ``True`` if the task registered under ``name`` is asynchronous."""

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

        info = self._tasks.get(name)
        if not info:
            raise ValueError(f"Unknown task: {name}")
        if info["disabled"]:
            raise ValueError(f"Task '{name}' is disabled")
        if info.get("paused"):
            raise ValueError(f"Task '{name}' is paused")
        task = info["task"]

        if (use_temporal or (use_temporal is None and self._temporal)):
            if not self._temporal:
                raise RuntimeError("Temporal backend not configured")
            workflow = getattr(task, "workflow", task.__class__.__name__)
            return self._temporal.run_workflow_sync(workflow)

        if hasattr(task, "run"):
            from datetime import datetime
            from uuid import uuid4

            from ..ume import emit_task_run
            from ..ume.models import TaskRun, TaskSpec
            from ..orchestrator import TaskPipeline
            from ..pipeline_registry import add_pipeline, remove_pipeline

            spec = TaskSpec(id=task.__class__.__name__, name=task.__class__.__name__)

            @metrics.track_task(name=task.__class__.__name__)
            def runner():
                run_id = str(uuid4())
                started = Timestamp()
                started.FromDatetime(datetime.now())
                status = "success"
                try:
                    if any(
                        hasattr(task, attr)
                        for attr in ("intake", "research", "plan", "verify")
                    ):
                        pipeline = TaskPipeline(task)
                        add_pipeline(name, pipeline)
                        try:
                            result = pipeline.run(user_id=user_id, group_id=group_id)
                            if inspect.iscoroutine(result):
                                try:
                                    asyncio.get_running_loop()
                                except RuntimeError:
                                    result = asyncio.run(result)
                                else:
                                    _res = result
                                    async def _await_result() -> Any:
                                        return await _res
                                    result = _await_result()
                        finally:
                            remove_pipeline(name)
                    else:
                        result = task.run()
                        if inspect.iscoroutine(result):
                            try:
                                asyncio.get_running_loop()
                            except RuntimeError:
                                result = asyncio.run(result)
                            else:
                                _res = result
                                async def _await_result() -> Any:
                                    return await _res
                                result = _await_result()
                except Exception:
                    status = "error"
                    raise
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
                return result

            return runner()
        raise AttributeError(f"Task '{name}' has no run() method")

    def replay_history(self, history_path: str) -> None:
        """Replay a workflow history using the configured Temporal backend."""

        if not self._temporal:
            raise RuntimeError("Temporal backend not configured")
        self._temporal.replay(history_path)

    def disable_task(self, name: str) -> None:
        """Disable a registered task."""

        if name not in self._tasks:
            raise ValueError(f"Unknown task: {name}")
        self._tasks[name]["disabled"] = True

    def pause_task(self, name: str) -> None:
        """Temporarily pause a registered task."""

        if name not in self._tasks:
            raise ValueError(f"Unknown task: {name}")
        self._tasks[name]["paused"] = True

    def resume_task(self, name: str) -> None:
        """Resume a previously paused task."""

        if name not in self._tasks:
            raise ValueError(f"Unknown task: {name}")
        self._tasks[name]["paused"] = False


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
        self.schedules = self._load_schedules()
        self._restore_jobs(tasks or {})

    def _load_schedules(self):
        if self.storage_path.exists():
            with open(self.storage_path, "r") as fh:
                data = self._yaml.safe_load(fh) or {}
                if isinstance(data, dict):
                    return data
        return {}

    def _restore_jobs(self, tasks):
        for job_id, data in self.schedules.items():
            task = tasks.get(job_id)
            if not task:
                continue
            if isinstance(data, dict):
                expr = data.get("expr")
                user_id = data.get("user_id")
                group_id = data.get("group_id")
            else:
                expr = data
                user_id = None
                group_id = None
            super().register_task(job_id, task)
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

    def _wrap_task(
        self, task, user_id: str | None = None, group_id: str | None = None
    ):
        def runner():
            info = self._tasks.get(task.__class__.__name__)
            if info and info.get("paused"):
                return
            self.run_task(
                task.__class__.__name__, user_id=user_id, group_id=group_id
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

        if isinstance(name_or_task, str):
            # Called with ``name`` and ``task``
            name, task = name_or_task, task_or_expr
            super().register_task(name, task)
            return

        task, cron_expression = name_or_task, task_or_expr
        job_id = task.__class__.__name__
        super().register_task(job_id, task)
        if user_id is None and group_id is None:
            self.schedules[job_id] = cron_expression
        else:
            entry: dict[str, Any] = {"expr": cron_expression}
            if user_id is not None:
                entry["user_id"] = user_id
            if group_id is not None:
                entry["group_id"] = group_id
            self.schedules[job_id] = entry
        self._save_schedules()

        trigger = self._CronTrigger.from_crontab(
            cron_expression, timezone=self.scheduler.timezone
        )
        self.scheduler.add_job(
            self._wrap_task(task, user_id=user_id, group_id=group_id),
            trigger=trigger,
            id=job_id,
            replace_existing=True,
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

    def unschedule(self, name: str) -> None:
        """Remove the cron schedule for ``name``."""

        if name not in self.schedules:
            raise ValueError(f"Unknown schedule: {name}")
        try:
            self.scheduler.remove_job(name)
        except Exception:  # pragma: no cover - passthrough
            pass
        self.schedules.pop(name, None)
        self._save_schedules()
        from ..ume import emit_stage_update

        emit_stage_update(name, "unschedule")

    def pause_task(self, name: str) -> None:
        """Pause ``name`` and emit a stage event."""

        super().pause_task(name)
        from ..ume import emit_stage_update

        emit_stage_update(name, "paused")

    def resume_task(self, name: str) -> None:
        """Resume ``name`` and emit a stage event."""

        super().resume_task(name)
        from ..ume import emit_stage_update

        emit_stage_update(name, "resumed")


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



