from __future__ import annotations

from typing import Any, Dict, Iterable

from . import CronScheduler


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
        """Restore persisted DAG schedules including dependency metadata."""

        super()._restore_jobs(tasks)

        with self._schedules_lock:
            items = list(self.schedules.items())

        for job_id, data in items:
            if not isinstance(data, dict):
                self.dependencies.pop(job_id, None)
                continue

            raw_deps = data.get("deps")
            if isinstance(raw_deps, (list, tuple)):
                deps = [str(dep) for dep in raw_deps]
                if deps:
                    self.dependencies[job_id] = deps
                else:
                    self.dependencies.pop(job_id, None)
            else:
                self.dependencies.pop(job_id, None)

    # ------------------------------------------------------------------
    def register_task(
        self,
        name_or_task: str | Any,
        task_or_expr: Any | str,
        *,
        dependencies: Iterable[str] | None = None,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> None:
        if isinstance(name_or_task, str):
            name, task = name_or_task, task_or_expr
            super().register_task(name, task)
            if dependencies:
                self.dependencies[name] = list(dependencies)
            else:
                self.dependencies.pop(name, None)
            return

        task, cron_expression = name_or_task, task_or_expr
        job_id = task.__class__.__name__

        super().register_task(
            task,
            cron_expression,
            user_id=user_id,
            group_id=group_id,
        )

        if dependencies:
            self.dependencies[job_id] = list(dependencies)
        else:
            self.dependencies.pop(job_id, None)

        with self._schedules_lock:
            entry = self.schedules.get(job_id)
            if isinstance(entry, dict):
                if dependencies:
                    entry["deps"] = list(dependencies)
                elif "deps" in entry:
                    entry.pop("deps", None)
                self._save_schedules()

    # ------------------------------------------------------------------
    def _run_with_dependencies(
        self,
        name: str,
        executed: set[str],
        *,
        use_temporal: bool | None,
        user_id: str | None,
        group_id: str | None,
        stack: list[str] | None = None,
    ) -> Any:
        if stack is None:
            stack = []
        if name in executed:
            return None
        if name in stack:
            cycle = " -> ".join(stack + [name])
            raise ValueError(f"Cyclic dependency detected: {cycle}")
        stack.append(name)
        try:
            for dep in self.dependencies.get(name, []):
                self._run_with_dependencies(
                    dep,
                    executed,
                    use_temporal=use_temporal,
                    user_id=user_id,
                    group_id=group_id,
                    stack=stack,
                )
        finally:
            stack.pop()
        executed.add(name)
        return super().run_task(
            name, use_temporal=use_temporal, user_id=user_id, group_id=group_id
        )

    def run_task(
        self,
        name: str,
        *,
        use_temporal: bool | None = None,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> Any:
        executed: set[str] = set()
        return self._run_with_dependencies(
            name,
            executed,
            use_temporal=use_temporal,
            user_id=user_id,
            group_id=group_id,
        )

    def _wrap_task(
        self, task: Any, user_id: str | None = None, group_id: str | None = None
    ):
        def runner():
            info = self._tasks.get(task.__class__.__name__)
            if info and info.get("paused"):
                return
            self.run_task(
                task.__class__.__name__, user_id=user_id, group_id=group_id
            )

        return runner

    # ------------------------------------------------------------------
    def unschedule(self, name: str) -> None:
        """Remove ``name`` from schedules and dependencies."""

        super().unschedule(name)
        self.dependencies.pop(name, None)
