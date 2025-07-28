from __future__ import annotations

from typing import Any, Dict, Iterable

from . import CronScheduler, BaseScheduler


class DagCronScheduler(CronScheduler):
    """Cron scheduler supporting task dependencies."""

    def __init__(
        self,
        *,
        dependencies: Dict[str, Iterable[str]] | None = None,
        **kwargs: Any,
    ) -> None:
        self.dependencies: Dict[str, list[str]] = {}
        self._initial_dependencies = {
            k: list(v) for k, v in (dependencies or {}).items()
        }
        super().__init__(**kwargs)
        self.dependencies.update(self._initial_dependencies)

    # ------------------------------------------------------------------
    def _restore_jobs(self, tasks: Dict[str, Any]) -> None:
        for job_id, data in self.schedules.items():
            task = tasks.get(job_id)
            if not task:
                continue
            if isinstance(data, dict):
                expr = data.get("expr")
                user_id = data.get("user_id")
                deps = data.get("deps", [])
            else:
                expr = data
                user_id = None
                deps = []
            if deps:
                self.dependencies[job_id] = list(deps)
            BaseScheduler.register_task(self, job_id, task)
            trigger = self._CronTrigger.from_crontab(
                expr, timezone=self.scheduler.timezone
            )
            self.scheduler.add_job(
                self._wrap_task(task, user_id=user_id),
                trigger=trigger,
                id=job_id,
                replace_existing=True,
            )

    # ------------------------------------------------------------------
    def register_task(
        self,
        name_or_task: str | Any,
        task_or_expr: Any | str,
        *,
        dependencies: Iterable[str] | None = None,
        user_id: str | None = None,
    ) -> None:
        if isinstance(name_or_task, str):
            name, task = name_or_task, task_or_expr
            super().register_task(name, task)
            if dependencies:
                self.dependencies[name] = list(dependencies)
            return

        task, cron_expression = name_or_task, task_or_expr
        job_id = task.__class__.__name__
        BaseScheduler.register_task(self, job_id, task)
        if dependencies:
            self.dependencies[job_id] = list(dependencies)
        entry: Dict[str, Any] = {"expr": cron_expression}
        if user_id is not None:
            entry["user_id"] = user_id
        if dependencies:
            entry["deps"] = list(dependencies)
        self.schedules[job_id] = entry
        self._save_schedules()
        trigger = self._CronTrigger.from_crontab(
            cron_expression, timezone=self.scheduler.timezone
        )
        self.scheduler.add_job(
            self._wrap_task(task, user_id=user_id),
            trigger=trigger,
            id=job_id,
            replace_existing=True,
        )

    # ------------------------------------------------------------------
    def _run_with_dependencies(
        self,
        name: str,
        executed: set[str],
        *,
        use_temporal: bool | None,
        user_id: str | None,
    ) -> Any:
        if name in executed:
            return None
        for dep in self.dependencies.get(name, []):
            self._run_with_dependencies(
                dep,
                executed,
                use_temporal=use_temporal,
                user_id=user_id,
            )
        executed.add(name)
        return super().run_task(
            name, use_temporal=use_temporal, user_id=user_id
        )

    def run_task(
        self,
        name: str,
        *,
        use_temporal: bool | None = None,
        user_id: str | None = None,
    ) -> Any:
        executed: set[str] = set()
        return self._run_with_dependencies(
            name,
            executed,
            use_temporal=use_temporal,
            user_id=user_id,
        )

    def _wrap_task(self, task: Any, user_id: str | None = None):
        def runner():
            info = self._tasks.get(task.__class__.__name__)
            if info and info.get("paused"):
                return
            self.run_task(task.__class__.__name__, user_id=user_id)

        return runner

    # ------------------------------------------------------------------
    def unschedule(self, name: str) -> None:
        """Remove ``name`` from schedules and dependencies."""

        super().unschedule(name)
        self.dependencies.pop(name, None)
