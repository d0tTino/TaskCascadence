"""Task orchestration pipeline."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Coroutine, cast
from uuid import uuid4
import asyncio
import inspect

try:
    import ai_plan  # type: ignore
except Exception:  # pragma: no cover - optional dependency may be missing
    ai_plan = None  # type: ignore

from google.protobuf.timestamp_pb2 import Timestamp

from .ume import emit_task_spec, emit_task_run, emit_stage_update_event
from .ume.models import TaskRun, TaskSpec
from . import research
import time


class PrecheckError(RuntimeError):
    """Raised when a task precheck fails."""


@dataclass
class ParallelPlan:
    """Container for a group of subtasks that should run concurrently."""

    tasks: list[Any]


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
        emit_stage_update_event(
            self.task.__class__.__name__, stage, user_id=user_id
        )

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
                try:
                    result = await research.async_gather(q)
                except RuntimeError:
                    self._emit_stage("research", user_id)
                    return None
                self._emit_stage("research", user_id)
                return result

            if loop_running:
                return _await_query()
            return asyncio.run(_await_query())

        if loop_running:
            async def _async_call() -> Any:
                try:
                    result = await research.async_gather(query)
                except RuntimeError:
                    self._emit_stage("research", user_id)
                    return None
                self._emit_stage("research", user_id)
                return result

            return _async_call()

        try:
            result = research.gather(query)
        except RuntimeError:
            self._emit_stage("research", user_id)
            return None
        self._emit_stage("research", user_id)
        return result

    def plan(self, *, user_id: str | None = None) -> Any:
        """Return a plan which may include subtasks."""

        plan_result = None
        if hasattr(self.task, "plan"):
            plan_result = self.task.plan()
        elif ai_plan is not None and hasattr(ai_plan, "plan"):
            plan_result = ai_plan.plan(self.task)
        self._emit_stage("planning", user_id)
        return plan_result

    def execute(self, plan_result: Any = None, *, user_id: str | None = None) -> Any:
        """Run the task or any planned subtasks."""

        start_ts = Timestamp()
        start_ts.FromDatetime(datetime.now())
        status = "success"

        try:
            if hasattr(self.task, "precheck"):
                check = self.task.precheck()
                if inspect.isawaitable(check):
                    try:
                        asyncio.get_running_loop()
                    except RuntimeError:
                        check = asyncio.run(cast(Coroutine[Any, Any, Any], check))
                    else:
                        _res = check

                        async def _await_precheck(res=_res) -> Any:
                            return await res

                        check = _await_precheck()
                if check is not True:
                    status = "error"
                    raise PrecheckError("precheck failed")
                self._emit_stage("precheck", user_id)

            parallel_tasks: list[Any] | None = None
            if isinstance(plan_result, dict) and plan_result.get("execution") == "parallel":
                parallel_tasks = cast(list[Any], plan_result.get("tasks", []))
            elif isinstance(plan_result, ParallelPlan):
                parallel_tasks = plan_result.tasks

            if parallel_tasks is not None:
                pipelines = [p if isinstance(p, TaskPipeline) else TaskPipeline(p) for p in parallel_tasks]

                async def _run_all() -> list[Any]:
                    async def _one(p: TaskPipeline) -> Any:
                        r = p.run(user_id=user_id)
                        if inspect.isawaitable(r):
                            return await r
                        return r

                    return await asyncio.gather(*[_one(pl) for pl in pipelines])

                results: Any
                try:
                    asyncio.get_running_loop()
                except RuntimeError:
                    results = asyncio.run(_run_all())
                else:
                    async def _await_all() -> list[Any]:
                        return await _run_all()

                    results = _await_all()

                result = results
                if hasattr(self.task, "run"):
                    result = self._call_run(results)
            elif isinstance(plan_result, list):
                async def _run_all() -> list[Any]:
                    res: list[Any] = []
                    for sub in plan_result:
                        pipeline = sub if isinstance(sub, TaskPipeline) else TaskPipeline(sub)
                        sub_result = pipeline.run(user_id=user_id)
                        if inspect.isawaitable(sub_result):
                            sub_result = await cast(Coroutine[Any, Any, Any], sub_result)
                        res.append(sub_result)
                    return res

                async def _resolve_and_run() -> Any:
                    res = await _run_all()
                    if hasattr(self.task, "run"):
                        parent_res = self._call_run(res)
                        if inspect.isawaitable(parent_res):
                            parent_res = await cast(Coroutine[Any, Any, Any], parent_res)
                        return parent_res
                    return res

                try:
                    asyncio.get_running_loop()
                except RuntimeError:
                    result = asyncio.run(_resolve_and_run())
                else:
                    result = _resolve_and_run()
            else:
                result = self._call_run(plan_result)

            emit_stage_update_event(
                self.task.__class__.__name__, "run", user_id=user_id
            )
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
            if inspect.isawaitable(verify_result):
                try:
                    asyncio.get_running_loop()
                except RuntimeError:
                    verify_result = asyncio.run(cast(Coroutine[Any, Any, Any], verify_result))
                else:
                    _res = verify_result

                    async def _await_verify(res=_res) -> Any:
                        return await res

                    verify_result = _await_verify()
        self._emit_stage("verification", user_id)
        return verify_result

    # ------------------------------------------------------------------
    def pause(self, *, user_id: str | None = None) -> None:
        """Pause execution of this pipeline."""
        self._paused = True
        emit_stage_update_event(
            self.task.__class__.__name__, "paused", user_id=user_id
        )

    def resume(self, *, user_id: str | None = None) -> None:
        """Resume a previously paused pipeline."""
        self._paused = False
        emit_stage_update_event(
            self.task.__class__.__name__, "resumed", user_id=user_id
        )

    def _wait_if_paused(self) -> Any:
        """Block until the pipeline is resumed."""

        loop_running = True
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            loop_running = False

        if loop_running:
            async def _async_wait() -> None:
                while self._paused:
                    await asyncio.sleep(0.1)

            return _async_wait()

        while self._paused:
            time.sleep(0.1)
        return None

    async def _wait_if_paused_async(self) -> None:
        """Async variant of :meth:`_wait_if_paused`."""
        while self._paused:
            await asyncio.sleep(0.1)

    # ------------------------------------------------------------------
    def run(self, *, user_id: str | None = None) -> Any:
        loop_running = True
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            loop_running = False

        if loop_running:
            async def _async_run() -> Any:
                self.intake(user_id=user_id)
                wait = self._wait_if_paused()
                if inspect.isawaitable(wait):
                    await wait
                else:
                    assert wait is None

                result = self.research(user_id=user_id)
                if inspect.isawaitable(result):
                    await result

                wait = self._wait_if_paused()
                if inspect.isawaitable(wait):
                    await wait

                plan_result = self.plan(user_id=user_id)
                if inspect.isawaitable(plan_result):
                    plan_result = await plan_result

                wait = self._wait_if_paused()
                if inspect.isawaitable(wait):
                    await wait

                exec_result = self.execute(plan_result, user_id=user_id)
                if inspect.isawaitable(exec_result):
                    exec_result = await exec_result

                wait = self._wait_if_paused()
                if inspect.isawaitable(wait):
                    await wait

                verify_result = self.verify(exec_result, user_id=user_id)
                if inspect.isawaitable(verify_result):
                    verify_result = await verify_result
                return verify_result

            return _async_run()

        self.intake(user_id=user_id)
        self._wait_if_paused()
        self.research(user_id=user_id)
        self._wait_if_paused()
        plan_result = self.plan(user_id=user_id)
        self._wait_if_paused()
        exec_result = self.execute(plan_result, user_id=user_id)
        self._wait_if_paused()
        return self.verify(exec_result, user_id=user_id)

    async def run_async(self, *, user_id: str | None = None) -> Any:
        """Asynchronously execute this pipeline."""

        self.intake(user_id=user_id)
        await self._wait_if_paused_async()

        research_result = self.research(user_id=user_id)
        if inspect.isawaitable(research_result):
            research_result = await research_result
        await self._wait_if_paused_async()

        plan_result = self.plan(user_id=user_id)
        if inspect.isawaitable(plan_result):
            plan_result = await plan_result
        await self._wait_if_paused_async()

        exec_result = self.execute(plan_result, user_id=user_id)
        if inspect.isawaitable(exec_result):
            exec_result = await exec_result
        await self._wait_if_paused_async()

        verify_result = self.verify(exec_result, user_id=user_id)
        if inspect.isawaitable(verify_result):
            verify_result = await verify_result
        return verify_result

    def _call_run(self, plan_result: Any) -> Any:
        """Execute the task's ``run`` method."""
        import inspect

        sig = inspect.signature(self.task.run)

        async def _async_call() -> Any:
            if len(sig.parameters) > 0:
                return await self.task.run(plan_result)
            return await self.task.run()

        if inspect.iscoroutinefunction(self.task.run):
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return asyncio.run(_async_call())
            return _async_call()

        if len(sig.parameters) > 0:
            return self.task.run(plan_result)
        return self.task.run()
