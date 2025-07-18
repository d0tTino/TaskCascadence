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

        self._tasks[name] = {"task": task, "disabled": False}

    # ------------------------------------------------------------------
    # Query helpers
    def list_tasks(self) -> Iterable[Tuple[str, bool]]:
        """Return an iterable of ``(name, disabled)`` tuples."""

        for name, info in self._tasks.items():
            yield name, info["disabled"]

    def run_task(
        self, name: str, *, use_temporal: bool | None = None
    ) -> Any:
        """Run a task by name if it exists and is enabled."""

        info = self._tasks.get(name)
        if not info:
            raise ValueError(f"Unknown task: {name}")
        if info["disabled"]:
            raise ValueError(f"Task '{name}' is disabled")
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

            spec = TaskSpec(id=task.__class__.__name__, name=task.__class__.__name__)
            run_id = str(uuid4())
            started = datetime.now()
            status = "success"
            try:
                result = task.run()
            except Exception:
                status = "error"
                raise
            finally:
                finished = datetime.now()
                run = TaskRun(
                    spec=spec,
                    run_id=run_id,
                    status=status,
                    started_at=started,
                    finished_at=finished,
                )
                emit_task_run(run)
            return result
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
        for job_id, expr in self.schedules.items():
            task = tasks.get(job_id)
            if not task:
                continue
            super().register_task(job_id, task)
            trigger = self._CronTrigger.from_crontab(
                expr, timezone=self.scheduler.timezone
            )
            self.scheduler.add_job(
                self._wrap_task(task), trigger=trigger, id=job_id
            )

    def _save_schedules(self):
        with open(self.storage_path, "w") as fh:
            self._yaml.safe_dump(self.schedules, fh)

    def _wrap_task(self, task):
        @metrics.track_task
        def runner():
            from datetime import datetime
            from uuid import uuid4

            from ..ume import emit_task_run
            from ..ume.models import TaskRun, TaskSpec

            spec = TaskSpec(
                id=task.__class__.__name__, name=task.__class__.__name__
            )
            run_id = str(uuid4())
            started = datetime.now()
            status = "success"
            try:
                task.run()
            except Exception:  # pragma: no cover - passthrough
                status = "error"
                raise
            finally:
                finished = datetime.now()
                run = TaskRun(
                    spec=spec,
                    run_id=run_id,
                    status=status,
                    started_at=started,
                    finished_at=finished,
                )
                emit_task_run(run)

        return runner

    def register_task(self, arg1, arg2):
        """Register a task with optional scheduling.

        This method supports two calling styles for backwards
        compatibility with :class:`BaseScheduler`:

        ``register_task(name, task)``
            Register ``task`` under ``name`` without scheduling.

        ``register_task(task, cron_expression)``
            Register ``task`` and schedule it using ``cron_expression``.
        """

        if isinstance(arg1, str):
            # Called with ``name`` and ``task``
            name, task = arg1, arg2
            super().register_task(name, task)
            return

        task, cron_expression = arg1, arg2
        job_id = task.__class__.__name__
        super().register_task(job_id, task)
        self.schedules[job_id] = cron_expression
        self._save_schedules()

        trigger = self._CronTrigger.from_crontab(
            cron_expression, timezone=self.scheduler.timezone
        )
        self.scheduler.add_job(
            self._wrap_task(task), trigger=trigger, id=job_id
        )

    def schedule_task(self, task: Any, cron_expression: str) -> None:
        """Convenience wrapper for :meth:`register_task`."""
        self.register_task(task, cron_expression)

    def start(self):
        self.scheduler.start()

    def shutdown(self, wait=True):
        self.scheduler.shutdown(wait=wait)

    def list_jobs(self):
        return self.scheduler.get_jobs()


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


