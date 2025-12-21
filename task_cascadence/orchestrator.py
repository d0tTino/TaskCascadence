"""Task orchestration pipeline."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from threading import Event, Lock
from typing import Any, Coroutine, Deque, TypedDict, cast
from uuid import uuid4
import asyncio
import inspect
import json

try:
    import ai_plan  # type: ignore
except Exception:  # pragma: no cover - optional dependency may be missing
    ai_plan = None  # type: ignore

from google.protobuf.timestamp_pb2 import Timestamp
from pydantic import ValidationError

from .ume import (
    emit_task_spec,
    emit_task_run,
    emit_stage_update_event,
    emit_audit_log,
)
from .ume.models import TaskRun, TaskSpec
from . import research
from .async_utils import run_coroutine
import time

MAX_CONTEXT_PAYLOAD_BYTES = 3072
AUDIT_CONTEXT_PREVIEW_LENGTH = 256


class EventKwargs(TypedDict, total=False):
    user_id: str
    group_id: str
    run_id: str


class SpecKwargs(TypedDict, total=False):
    user_id: str
    group_id: str


class PrecheckError(RuntimeError):
    """Raised when a task precheck fails."""


def _normalize_context_payload(payload: Any) -> Any:
    """Ensure *payload* is safe to queue and below size limits."""

    normalized = payload
    if hasattr(payload, "dict") and not isinstance(payload, dict):
        try:
            normalized = payload.dict(exclude_none=True)
        except Exception:
            normalized = payload

    if isinstance(normalized, dict) and ("note" in normalized or "url" in normalized):
        try:
            from task_cascadence.api import SignalContextValue

            context_model = SignalContextValue.model_validate(normalized)
            normalized = context_model.model_dump(exclude_none=True)
        except (ValidationError, Exception) as exc:  # pragma: no cover - converted below
            raise ValueError(str(getattr(exc, "detail", exc))) from exc
    elif isinstance(normalized, dict):
        normalized = dict(normalized)

    serialized = json.dumps(normalized, default=str, ensure_ascii=False)
    if len(serialized) > MAX_CONTEXT_PAYLOAD_BYTES:
        raise ValueError("context payload too large")

    return normalized


def _redact_context_for_audit(payload: Any) -> str | None:
    """Return a redacted, size-limited representation for audit logs."""

    if payload is None:
        return None

    if isinstance(payload, dict):
        redacted: dict[str, str] = {}
        if "note" in payload and payload.get("note"):
            note = str(payload["note"])
            preview = note[:AUDIT_CONTEXT_PREVIEW_LENGTH]
            if len(note) > AUDIT_CONTEXT_PREVIEW_LENGTH:
                preview += "..."
            redacted["note"] = preview
        if "url" in payload and payload.get("url"):
            url = str(payload["url"])
            preview = url[:AUDIT_CONTEXT_PREVIEW_LENGTH]
            if len(url) > AUDIT_CONTEXT_PREVIEW_LENGTH:
                preview += "..."
            redacted["url"] = preview
        if not redacted:
            return None
        return repr(redacted)

    text = str(payload)
    if not text:
        return None
    preview = text[:AUDIT_CONTEXT_PREVIEW_LENGTH]
    if len(text) > AUDIT_CONTEXT_PREVIEW_LENGTH:
        preview += "..."
    return repr(preview)


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
    _context_queue: Deque[Any] = field(default_factory=deque, init=False, repr=False)
    _context_lock: Lock = field(default_factory=Lock, init=False, repr=False)
    current_run_id: str | None = field(default=None, init=False)
    _is_running: bool = field(default=False, init=False, repr=False)
    _pending_context_delivery: tuple[str, str, str | None] | None = field(
        default=None, init=False, repr=False
    )

    def _emit_stage(
        self,
        stage: str,
        user_id: str | None,
        group_id: str | None = None,
    ) -> None:
        spec = TaskSpec(
            id=self.task.__class__.__name__,
            name=self.task.__class__.__name__,
            description=stage,
        )
        spec_kwargs: SpecKwargs = {}
        if user_id is not None:
            spec_kwargs["user_id"] = user_id
        if group_id is not None:
            spec_kwargs["group_id"] = group_id
        emit_task_spec(spec, **spec_kwargs)
        emit_stage_update_event(
            self.task.__class__.__name__,
            stage,
            **self._event_kwargs(user_id, group_id),
        )

    _context: deque[Any] = field(default_factory=deque, init=False, repr=False)
    _context_store: list[Any] = field(default_factory=list, init=False, repr=False)


    def _reset_run_context(self) -> None:
        """Initialise the per-run context container."""

        self._context_store.clear()
        self.task.context = self._context_store

    def _process_pending_context(self) -> None:
        """Attach any queued context to the task."""

        updated = False
        while self._context:
            self._context_store.append(self._context.popleft())
            updated = True
        if updated or not hasattr(self.task, "context"):
            self.task.context = self._context_store

    def _event_kwargs(
        self, user_id: str | None, group_id: str | None
    ) -> EventKwargs:
        kwargs: EventKwargs = {}
        if user_id is not None:
            kwargs["user_id"] = user_id
        if group_id is not None:
            kwargs["group_id"] = group_id
        if self.current_run_id is not None:
            kwargs["run_id"] = self.current_run_id
        return kwargs

    def intake(self, *, user_id: str, group_id: str | None = None) -> None:
        task_name = self.task.__class__.__name__
        emit_audit_log(
            task_name,
            "intake",
            "started",
            **self._event_kwargs(user_id, group_id),
        )
        result: Any | None = None
        try:
            if hasattr(self.task, "intake"):
                result = self.task.intake()
        except Exception as exc:
            self._emit_stage("intake", user_id, group_id)
            partial = result if result is not None else getattr(exc, "partial", None)
            emit_audit_log(
                task_name,
                "intake",
                "error",
                reason=str(exc),
                output=repr(partial) if partial is not None else None,
                **self._event_kwargs(user_id, group_id),
            )
            raise
        self._emit_stage("intake", user_id, group_id)
        emit_audit_log(
            task_name,
            "intake",
            "success",
            output=repr(result) if result is not None else None,
            **self._event_kwargs(user_id, group_id),
        )

    def research(self, *, user_id: str, group_id: str | None = None) -> Any:
        """Perform optional research for the task."""
        task_name = self.task.__class__.__name__
        emit_audit_log(
            task_name,
            "research",
            "started",
            **self._event_kwargs(user_id, group_id),
        )
        if not hasattr(self.task, "research"):
            # Even if a task has no dedicated research step, emit the standard
            # stage notification so downstream consumers observe a "research"
            # phase was considered and skipped.
            self._emit_stage("research", user_id, group_id)
            emit_audit_log(
                task_name,
                "research",
                "skipped",
                **self._event_kwargs(user_id, group_id),
            )
            return None

        query = self.task.research()

        loop_running = True
        try:
            asyncio.current_task()
        except RuntimeError:
            loop_running = False

        def _log_success(res: Any) -> Any:
            self._emit_stage("research", user_id, group_id)
            emit_audit_log(
                task_name,
                "research",
                "success",
                output=repr(res) if res is not None else None,
                **self._event_kwargs(user_id, group_id),
            )
            return res

        def _log_error(exc: Exception, partial: Any | None = None) -> Any:
            self._emit_stage("research", user_id, group_id)
            audit_partial = partial if partial is not None else getattr(exc, "partial", None)
            emit_audit_log(
                task_name,
                "research",
                "error",
                reason=str(exc),
                output=repr(audit_partial) if audit_partial is not None else None,
                **self._event_kwargs(user_id, group_id),
            )
            return None

        if inspect.isawaitable(query):

            async def _await_query() -> Any:
                q: Any | None = None
                result: Any | None = None
                try:
                    q = await query
                    if inspect.isawaitable(q):
                        q = await q
                    result = await research.async_gather(
                        q, user_id=user_id, group_id=group_id
                    )
                except Exception as exc:  # pragma: no cover - network errors
                    return _log_error(exc, result if result is not None else q)
                return _log_success(result)

            if loop_running:
                return _await_query()
            return run_coroutine(_await_query())

        if loop_running:

            async def _async_call() -> Any:
                result: Any | None = None
                try:
                    result = await research.async_gather(
                        query, user_id=user_id, group_id=group_id
                    )
                except Exception as exc:  # pragma: no cover - network errors
                    return _log_error(exc, result)
                return _log_success(result)

            return _async_call()

        result: Any | None = None
        try:
            result = research.gather(query, user_id=user_id, group_id=group_id)
        except Exception as exc:  # pragma: no cover - network errors
            return _log_error(exc, result)
        return _log_success(result)

    def plan(self, *, user_id: str, group_id: str | None = None) -> Any:
        """Return a plan which may include subtasks."""
        task_name = self.task.__class__.__name__
        emit_audit_log(
            task_name,
            "plan",
            "started",
            **self._event_kwargs(user_id, group_id),
        )
        plan_result: Any | None = None
        try:
            if hasattr(self.task, "plan"):
                plan_result = self.task.plan()
            elif ai_plan is not None and hasattr(ai_plan, "plan"):
                plan_result = ai_plan.plan(self.task)
        except Exception as exc:
            self._emit_stage("plan", user_id, group_id)
            partial = (
                plan_result
                if plan_result is not None
                else getattr(exc, "partial", None)
            )
            emit_audit_log(
                task_name,
                "plan",
                "error",
                reason=str(exc),
                output=repr(partial) if partial is not None else None,
                **self._event_kwargs(user_id, group_id),
            )
            raise
        self._emit_stage("plan", user_id, group_id)
        emit_audit_log(
            task_name,
            "plan",
            "success",
            output=repr(plan_result) if plan_result is not None else None,
            **self._event_kwargs(user_id, group_id),
        )
        return plan_result

    def execute(
        self,
        plan_result: Any = None,
        *,
        user_id: str,
        group_id: str | None = None,
    ) -> Any:
        """Run the task or any planned subtasks."""

        start_ts = Timestamp()
        start_ts.FromDatetime(datetime.now())
        status = "success"
        result: Any | None = None
        task_name = self.task.__class__.__name__
        if self.current_run_id is None:
            self.current_run_id = str(uuid4())
        emit_audit_log(
            task_name,
            "execute",
            "started",
            **self._event_kwargs(user_id, group_id),
        )

        try:
            if hasattr(self.task, "precheck"):
                emit_audit_log(
                    task_name,
                    "precheck",
                    "started",
                    **self._event_kwargs(user_id, group_id),
                )
                try:
                    check = self.task.precheck()
                    if inspect.isawaitable(check):
                        try:
                            asyncio.get_running_loop()
                        except RuntimeError:
                            check = run_coroutine(cast(Coroutine[Any, Any, Any], check))
                        else:
                            _res = check

                            async def _await_precheck(res=_res) -> Any:
                                return await res

                            check = _await_precheck()
                    if check is not True:
                        status = "error"
                        self._emit_stage("precheck", user_id, group_id)
                        reason = check if isinstance(check, str) else "precheck failed"
                        emit_audit_log(
                            task_name,
                            "precheck",
                            "error",
                            reason=str(reason),
                            **self._event_kwargs(user_id, group_id),
                        )
                        raise PrecheckError(str(reason))
                except Exception as exc:
                    status = "error"
                    self._emit_stage("precheck", user_id, group_id)
                    emit_audit_log(
                        task_name,
                        "precheck",
                        "error",
                        reason=str(exc),
                        **self._event_kwargs(user_id, group_id),
                    )
                    raise
                else:
                    self._emit_stage("precheck", user_id, group_id)
                    emit_audit_log(
                        task_name,
                        "precheck",
                        "success",
                        **self._event_kwargs(user_id, group_id),
                    )

            parallel_tasks: list[Any] | None = None
            if isinstance(plan_result, dict) and plan_result.get("execution") == "parallel":
                parallel_tasks = cast(list[Any], plan_result.get("tasks", []))
            elif isinstance(plan_result, ParallelPlan):
                parallel_tasks = plan_result.tasks

            if parallel_tasks is not None:
                pipelines = [p if isinstance(p, TaskPipeline) else TaskPipeline(p) for p in parallel_tasks]

                async def _run_all() -> list[Any]:
                    async def _one(p: TaskPipeline) -> Any:
                        if self.current_run_id is not None and p.current_run_id is None:
                            p.current_run_id = self.current_run_id
                        r = p.run(user_id=user_id, group_id=group_id)
                        if inspect.isawaitable(r):
                            return await r
                        return r

                    return await asyncio.gather(*[_one(pl) for pl in pipelines])

                results: Any
                try:
                    asyncio.get_running_loop()
                except RuntimeError:
                    results = run_coroutine(_run_all())
                else:
                    async def _await_all() -> list[Any]:
                        return await _run_all()

                    results = _await_all()

                result = results
                if hasattr(self.task, "run"):
                    result = self._call_run(results)
                emit_stage_update_event(
                    self.task.__class__.__name__,
                    "run",
                    **self._event_kwargs(user_id, group_id),
                )
            elif isinstance(plan_result, list):
                async def _run_all() -> list[Any]:
                    res: list[Any] = []
                    for sub in plan_result:
                        pipeline = sub if isinstance(sub, TaskPipeline) else TaskPipeline(sub)
                        if self.current_run_id is not None and pipeline.current_run_id is None:
                            pipeline.current_run_id = self.current_run_id
                        sub_result = pipeline.run(
                            user_id=user_id, group_id=group_id
                        )
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
                        emit_stage_update_event(
                            self.task.__class__.__name__,
                            "run",
                            **self._event_kwargs(user_id, group_id),
                        )
                        return parent_res
                    emit_stage_update_event(
                        self.task.__class__.__name__,
                        "run",
                        **self._event_kwargs(user_id, group_id),
                    )
                    return res

                try:
                    asyncio.get_running_loop()
                except RuntimeError:
                    result = run_coroutine(_resolve_and_run())
                else:
                    result = _resolve_and_run()
            else:
                result = self._call_run(plan_result)

                emit_stage_update_event(
                    self.task.__class__.__name__,
                    "run",
                    **self._event_kwargs(user_id, group_id),
                )
        except Exception as exc:
            status = "error"
            partial = result if result is not None else getattr(exc, "partial", None)
            emit_audit_log(
                self.task.__class__.__name__,
                "execute",
                "error",
                reason=str(exc),
                output=repr(partial) if partial is not None else None,
                **self._event_kwargs(user_id, group_id),
            )
            raise
        finally:
            end_ts = Timestamp()
            end_ts.FromDatetime(datetime.now())
            run = TaskRun(
                spec=TaskSpec(
                    id=self.task.__class__.__name__,
                    name=self.task.__class__.__name__,
                ),
                run_id=self.current_run_id,
                status=status,
                started_at=start_ts,
                finished_at=end_ts,
            )
            if group_id is None:
                emit_task_run(run, user_id=user_id)
            else:
                emit_task_run(run, user_id=user_id, group_id=group_id)
        emit_audit_log(
            self.task.__class__.__name__,
            "execute",
            status,
            output=repr(result) if status == "success" and result is not None else None,
            **self._event_kwargs(user_id, group_id),
        )
        return result

    def verify(
        self,
        exec_result: Any = None,
        *,
        user_id: str,
        group_id: str | None = None,
    ) -> Any:
        task_name = self.task.__class__.__name__
        emit_audit_log(
            task_name,
            "verify",
            "started",
            **self._event_kwargs(user_id, group_id),
        )
        verify_result = exec_result
        try:
            if hasattr(self.task, "verify"):
                verify_result = self.task.verify(exec_result)
                if inspect.isawaitable(verify_result):
                    try:
                        asyncio.get_running_loop()
                    except RuntimeError:
                        verify_result = run_coroutine(
                            cast(Coroutine[Any, Any, Any], verify_result)
                        )
                    else:
                        _res = verify_result

                        async def _await_verify(res=_res) -> Any:
                            return await res

                        verify_result = _await_verify()
        except Exception as exc:
            self._emit_stage("verify", user_id, group_id)
            partial = getattr(exc, "partial", None)
            if partial is None and verify_result is not None and verify_result is not exec_result:
                partial = verify_result
            emit_audit_log(
                task_name,
                "verify",
                "error",
                reason=str(exc),
                output=repr(partial) if partial is not None else None,
                **self._event_kwargs(user_id, group_id),
            )
            raise
        self._emit_stage("verify", user_id, group_id)
        emit_audit_log(
            task_name,
            "verify",
            "success",
            output=repr(verify_result) if verify_result is not None else None,
            **self._event_kwargs(user_id, group_id),
        )
        return verify_result

    # ------------------------------------------------------------------
    def pause(self, *, user_id: str, group_id: str | None = None) -> None:
        """Pause execution of this pipeline."""
        self._paused = True
        emit_stage_update_event(
            self.task.__class__.__name__,
            "paused",
            **self._event_kwargs(user_id, group_id),
        )

    def resume(self, *, user_id: str, group_id: str | None = None) -> None:
        """Resume a previously paused pipeline."""
        self._paused = False
        emit_stage_update_event(
            self.task.__class__.__name__,
            "resumed",
            **self._event_kwargs(user_id, group_id),
        )

    def attach_context(
        self,
        payload: Any,
        *,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> bool:
        """Queue *payload* to be delivered to the next pipeline stage."""

        try:
            stored_payload: Any = _normalize_context_payload(payload)
        except ValueError:
            raise

        immediate_stage: tuple[str, str, str | None] | None = None
        deliver_now = False
        with self._context_lock:
            self._context.append(stored_payload)
            self._context_queue.append(stored_payload)
            if (
                self._pending_context_delivery is not None
                and (not self._is_running or self._paused)
            ):
                immediate_stage = self._pending_context_delivery
                self._pending_context_delivery = None
            elif not self._is_running:
                deliver_now = True
                self._context_store.append(stored_payload)
                self.task.context = self._context_store

        resolved_user_id = (
            user_id if user_id is not None else getattr(self.task, "user_id", None)
        )
        resolved_group_id = (
            group_id if group_id is not None else getattr(self.task, "group_id", None)
        )

        if immediate_stage is not None:
            self._process_pending_context()
            stage, stage_user, stage_group = immediate_stage
            self._deliver_context(stage, user_id=stage_user, group_id=stage_group)
            consumed = True
        elif deliver_now:
            consumed = True
        else:
            consumed = False

        self._emit_stage("context_attached", resolved_user_id, resolved_group_id)
        emit_audit_log(
            self.task.__class__.__name__,
            "context_attached",
            "received",
            output=_redact_context_for_audit(stored_payload),
            **self._event_kwargs(resolved_user_id, resolved_group_id),
        )
        allow_event = getattr(self.task, "allow_plan", None)
        if isinstance(allow_event, Event):
            allow_event.set()
        elif isinstance(allow_event, asyncio.Event):
            allow_event.set()

        return consumed

    def _signal_ready_event(self, name: str) -> None:
        """Set an optional readiness event exposed by the task."""

        ready_obj = getattr(self.task, name, None)
        if ready_obj is None:
            return

        if isinstance(ready_obj, Event):
            ready_obj.set()
            return

        if isinstance(ready_obj, asyncio.Event):
            ready_obj.set()

    def _drain_context_queue(self) -> list[dict[str, Any]]:

        with self._context_lock:
            if not self._context_queue:
                return []
            queued = list(self._context_queue)
            self._context_queue.clear()
            return queued

    def _deliver_context(
        self,
        stage: str,
        *,
        user_id: str,
        group_id: str | None = None,
    ) -> None:
        contexts = self._drain_context_queue()
        if not contexts:
            return

        task_name = self.task.__class__.__name__
        for ctx in contexts:
            if hasattr(self.task, "attach_context"):
                self.task.attach_context(ctx)
            else:
                current = getattr(self.task, "context", None)
                if current is None:
                    self.task.context = ctx
                elif isinstance(current, list):
                    if current is not self._context_store:
                        current.append(ctx)
                else:
                    self.task.context = [current, ctx]

            emit_audit_log(
                task_name,
                f"{stage}.context",
                "consumed",
                output=_redact_context_for_audit(ctx),
                **self._event_kwargs(user_id, group_id),
            )

        if stage == "plan":
            allow_event = getattr(self.task, "allow_plan", None)
            if isinstance(allow_event, Event):
                allow_event.set()
            elif isinstance(allow_event, asyncio.Event):
                allow_event.set()

    def _schedule_context_delivery(
        self, stage: str, *, user_id: str, group_id: str | None
    ) -> None:
        with self._context_lock:
            self._pending_context_delivery = (stage, user_id, group_id)

    def _consume_pending_context(self) -> None:
        pending: tuple[str, str, str | None] | None
        with self._context_lock:
            pending = self._pending_context_delivery
            self._pending_context_delivery = None
        if pending is None:
            return
        stage, user_id, group_id = pending
        self._process_pending_context()
        self._deliver_context(stage, user_id=user_id, group_id=group_id)

    def _clear_pending_context(self) -> None:
        with self._context_lock:
            self._pending_context_delivery = None

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
                self._process_pending_context()

            return _async_wait()

        while self._paused:
            time.sleep(0.1)
        self._process_pending_context()
        return None

    async def _wait_if_paused_async(self) -> None:
        """Async variant of :meth:`_wait_if_paused`."""
        while self._paused:
            await asyncio.sleep(0.1)
        self._process_pending_context()

    # ------------------------------------------------------------------


    def run(self, *, user_id: str, group_id: str | None = None) -> Any:
        self.task.user_id = user_id
        self.task.group_id = group_id
        if self.current_run_id is None:
            self.current_run_id = str(uuid4())
        self._reset_run_context()
        self._process_pending_context()
        self._is_running = True
        self._signal_ready_event("research_ready")
        loop_running = True
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            loop_running = False

        if loop_running:

            async def _async_run() -> Any:
                try:
                    self._signal_ready_event("research_ready")
                    self._process_pending_context()
                    self.intake(user_id=user_id, group_id=group_id)
                    self._process_pending_context()
                    self._schedule_context_delivery(
                        "research", user_id=user_id, group_id=group_id
                    )
                    self._signal_ready_event("research_ready")
                    wait = self._wait_if_paused()
                    if inspect.isawaitable(wait):
                        await wait
                    else:
                        assert wait is None
                    self._consume_pending_context()

                    result = self.research(user_id=user_id, group_id=group_id)
                    if inspect.isawaitable(result):
                        await result

                    self._process_pending_context()
                    self._schedule_context_delivery(
                        "plan", user_id=user_id, group_id=group_id
                    )
                    wait = self._wait_if_paused()
                    if inspect.isawaitable(wait):
                        await wait
                    self._consume_pending_context()

                    plan_result = self.plan(user_id=user_id, group_id=group_id)
                    if inspect.isawaitable(plan_result):
                        plan_result = await plan_result

                    self._process_pending_context()
                    self._schedule_context_delivery(
                        "execute", user_id=user_id, group_id=group_id
                    )
                    wait = self._wait_if_paused()
                    if inspect.isawaitable(wait):
                        await wait
                    self._consume_pending_context()

                    exec_result = self.execute(
                        plan_result, user_id=user_id, group_id=group_id
                    )
                    if inspect.isawaitable(exec_result):
                        exec_result = await exec_result

                    self._process_pending_context()
                    self._schedule_context_delivery(
                        "verify", user_id=user_id, group_id=group_id
                    )
                    wait = self._wait_if_paused()
                    if inspect.isawaitable(wait):
                        await wait
                    self._consume_pending_context()

                    verify_result = self.verify(
                        exec_result, user_id=user_id, group_id=group_id
                    )
                    if inspect.isawaitable(verify_result):
                        verify_result = await verify_result
                    return verify_result
                finally:
                    self._is_running = False
                    self._clear_pending_context()

            return _async_run()

        try:
            self._process_pending_context()
            self.intake(user_id=user_id, group_id=group_id)
            self._process_pending_context()
            self._schedule_context_delivery(
                "research", user_id=user_id, group_id=group_id
            )
            self._signal_ready_event("research_ready")
            self._wait_if_paused()
            self._consume_pending_context()

            self.research(user_id=user_id, group_id=group_id)
            self._process_pending_context()
            self._schedule_context_delivery(
                "plan", user_id=user_id, group_id=group_id
            )
            self._wait_if_paused()
            self._consume_pending_context()

            plan_result = self.plan(user_id=user_id, group_id=group_id)
            self._process_pending_context()
            self._schedule_context_delivery(
                "execute", user_id=user_id, group_id=group_id
            )
            self._wait_if_paused()
            self._consume_pending_context()

            exec_result = self.execute(
                plan_result, user_id=user_id, group_id=group_id
            )
            self._process_pending_context()
            self._schedule_context_delivery(
                "verify", user_id=user_id, group_id=group_id
            )
            self._wait_if_paused()
            self._consume_pending_context()

            return self.verify(exec_result, user_id=user_id, group_id=group_id)
        finally:
            self._is_running = False
            self._clear_pending_context()

    async def run_async(
        self, *, user_id: str, group_id: str | None = None
    ) -> Any:
        """Asynchronously execute this pipeline."""
        self.task.user_id = user_id
        self.task.group_id = group_id
        if self.current_run_id is None:
            self.current_run_id = str(uuid4())
        self._reset_run_context()
        self._process_pending_context()
        self._signal_ready_event("research_ready")
        self._is_running = True
        self.intake(user_id=user_id, group_id=group_id)
        self._process_pending_context()
        try:
            self._schedule_context_delivery(
                "research", user_id=user_id, group_id=group_id
            )
            self._signal_ready_event("research_ready")
            await self._wait_if_paused_async()
            self._consume_pending_context()

            research_result = self.research(user_id=user_id, group_id=group_id)
            if inspect.isawaitable(research_result):
                research_result = await research_result
            self._process_pending_context()
            self._schedule_context_delivery(
                "plan", user_id=user_id, group_id=group_id
            )
            await self._wait_if_paused_async()
            self._consume_pending_context()

            plan_result = self.plan(user_id=user_id, group_id=group_id)
            if inspect.isawaitable(plan_result):
                plan_result = await plan_result
            self._process_pending_context()
            self._schedule_context_delivery(
                "execute", user_id=user_id, group_id=group_id
            )
            await self._wait_if_paused_async()
            self._consume_pending_context()

            exec_result = self.execute(
                plan_result, user_id=user_id, group_id=group_id
            )
            if inspect.isawaitable(exec_result):
                exec_result = await exec_result
            self._process_pending_context()
            self._schedule_context_delivery(
                "verify", user_id=user_id, group_id=group_id
            )
            await self._wait_if_paused_async()
            self._consume_pending_context()

            verify_result = self.verify(
                exec_result, user_id=user_id, group_id=group_id
            )
            if inspect.isawaitable(verify_result):
                verify_result = await verify_result
            self._process_pending_context()
            return verify_result
        finally:
            self._is_running = False
            self._clear_pending_context()


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
                return run_coroutine(_async_call())
            return _async_call()

        if len(sig.parameters) > 0:
            return self.task.run(plan_result)
        return self.task.run()
