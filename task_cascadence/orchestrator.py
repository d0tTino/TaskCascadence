"""Task orchestration pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import uuid4
import asyncio
import inspect

try:
    import ai_plan  # type: ignore
except Exception:  # pragma: no cover - optional dependency may be missing
    ai_plan = None  # type: ignore

from google.protobuf.timestamp_pb2 import Timestamp

from .ume import emit_task_spec, emit_task_run, emit_stage_update
from .ume.models import TaskRun, TaskSpec
from . import research
import time


@dataclass
class TaskPipeline:
    """Orchestrate a task through multiple stages.

    Each stage emits an event via :mod:`task_cascadence.ume`.
    """

    task: Any
    _paused: bool = field(default=False, init=False, repr=False)

    def _emit_stage(self, stage: str, user_id: str | None = None) -> None:
        spec = TaskSpec(
            id=self.task.__class__.__name__,
            name=self.task.__class__.__name__,
            description=stage,
        )
        emit_task_spec(spec, user_id=user_id)
        emit_stage_update(self.task.__class__.__name__, stage, user_id=user_id)

    def intake(self, *, user_id: str | None = None) -> None:
        if hasattr(self.task, "intake"):
            self.task.intake()
        self._emit_stage("intake", user_id)

    def research(self, *, user_id: str | None = None) -> Any:
        """Perform optional research for the task."""
        if not hasattr(self.task, "research"):
            return None

        query = self.task.research()

        loop_running = True
        try:
            asyncio.current_task()
        except RuntimeError:
            loop_running = False

        if inspect.isawaitable(query):
            async def _await_query() -> Any:
                q = await query
                if inspect.isawaitable(q):
                    q = await q
                result = await research.async_gather(q)
                self._emit_stage("research", user_id)
                return result

            if loop_running:
                return _await_query()
            return asyncio.run(_await_query())

        if loop_running:
            async def _async_call() -> Any:
                result = await research.async_gather(query)
                self._emit_stage("research", user_id)
                return result

            return _async_call()

        result = research.gather(query)
        self._emit_stage("research", user_id)
        return result

    def plan(self, *, user_id: str | None = None) -> Any:
        plan_result = None
        if hasattr(self.task, "plan"):
            plan_result = self.task.plan()
        elif ai_plan is not None and hasattr(ai_plan, "plan"):
            plan_result = ai_plan.plan(self.task)
        self._emit_stage("planning", user_id)
        return plan_result

    def execute(self, plan_result: Any = None, *, user_id: str | None = None) -> Any:
        start_ts = Timestamp()
        start_ts.FromDatetime(datetime.now())
        status = "success"
        try:
            result = self._call_run(plan_result)
            emit_stage_update(self.task.__class__.__name__, "run", user_id=user_id)
        except Exception:
            status = "error"
            raise
        finally:
            end_ts = Timestamp()
            end_ts.FromDatetime(datetime.now())
            run = TaskRun(
                spec=TaskSpec(
                    id=self.task.__class__.__name__,
                    name=self.task.__class__.__name__,
                ),
                run_id=str(uuid4()),
                status=status,
                started_at=start_ts,
                finished_at=end_ts,
            )
            emit_task_run(run, user_id=user_id)
        return result

    def verify(self, exec_result: Any = None, *, user_id: str | None = None) -> Any:
        verify_result = exec_result
        if hasattr(self.task, "verify"):
            verify_result = self.task.verify(exec_result)
        self._emit_stage("verification", user_id)
        return verify_result

    # ------------------------------------------------------------------
    def pause(self, *, user_id: str | None = None) -> None:
        """Pause execution of this pipeline."""
        self._paused = True
        emit_stage_update(self.task.__class__.__name__, "paused", user_id=user_id)

    def resume(self, *, user_id: str | None = None) -> None:
        """Resume a previously paused pipeline."""
        self._paused = False
        emit_stage_update(self.task.__class__.__name__, "resumed", user_id=user_id)

    def _wait_if_paused(self) -> None:
        while self._paused:
            time.sleep(0.1)

    # ------------------------------------------------------------------
    def run(self, *, user_id: str | None = None) -> Any:
        self.intake(user_id=user_id)
        self._wait_if_paused()
        self.research(user_id=user_id)
        self._wait_if_paused()
        plan_result = self.plan(user_id=user_id)
        self._wait_if_paused()
        exec_result = self.execute(plan_result, user_id=user_id)
        self._wait_if_paused()
        return self.verify(exec_result, user_id=user_id)

    def _call_run(self, plan_result: Any) -> Any:
        """Execute the task's ``run`` method."""
        import inspect

        sig = inspect.signature(self.task.run)

        if len(sig.parameters) > 0:
            return self.task.run(plan_result)
        return self.task.run()
