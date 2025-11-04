from __future__ import annotations

import inspect
import re
from typing import Any, Dict

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from ..scheduler import get_default_scheduler, CronScheduler
from ..stage_store import StageStore
from ..cli import _pointer_add, _pointer_list, _pointer_receive
from ..pipeline_registry import get_pipeline
from ..plugins import load_plugin
from ..task_store import TaskStore
from ..suggestions.engine import get_default_engine
from ..intent import resolve_intent, sanitize_input
from ..ume import _hash_user_id, emit_audit_log


_HASH_RE = re.compile(r"^[0-9a-f]{64}$")


def _ensure_hash(value: str) -> str:
    """Return ``value`` if already hashed otherwise return its hashed form."""

    return value if _HASH_RE.match(value) else _hash_user_id(value)

app = FastAPI()


class IntentRequest(BaseModel):
    """Schema for intent analysis requests."""

    message: str
    context: list[str] = Field(default_factory=list)


class IntentResponse(BaseModel):
    """Schema for responses returned by the intent endpoint."""

    task: str | None = None
    arguments: Dict[str, Any] = Field(default_factory=dict)
    confidence: float
    clarification: bool


class SignalPayload(BaseModel):
    """Schema for signals sent to a running pipeline."""

    kind: str
    value: Dict[str, Any]


def get_user_id(x_user_id: str | None = Header(default=None)) -> str:
    """Return the user identifier from ``X-User-ID`` header."""
    if x_user_id is None:
        raise HTTPException(400, "user_id header required")
    return x_user_id


def get_group_id(x_group_id: str | None = Header(default=None)) -> str:
    """Return the group identifier from ``X-Group-ID`` header."""
    if x_group_id is None:
        raise HTTPException(400, "group_id header required")
    return x_group_id


@app.get("/tasks")
def list_tasks(
    user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
):
    """Return all registered tasks."""
    sched = get_default_scheduler()
    tasks = [
        {"name": name, "disabled": disabled}
        for name, disabled in sched.list_tasks()
    ]

    try:  # pragma: no cover - audit logging should not break the endpoint
        emit_audit_log(
            "api",
            "tasks",
            "list",
            user_id=user_id,
            group_id=group_id,
            output=str(len(tasks)),
        )
    except Exception:  # pragma: no cover - optional audit logging
        pass

    return tasks


@app.post("/tasks")
def register_task(
    path: str,
    schedule: str | None = None,
    user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
):
    """Load a task plugin and optionally schedule it."""
    sched = get_default_scheduler()
    try:
        task = load_plugin(path)
        TaskStore().add_path(path)
        sched.register_task(task.name, task)
        if schedule:
            if isinstance(sched, CronScheduler):
                sched.register_task(task, schedule)
            elif hasattr(sched, "schedule_task"):
                sched.schedule_task(task.name, schedule)
            else:
                raise HTTPException(400, "scheduler lacks cron capabilities")
        return {"status": "registered", "name": task.name}
    except Exception as exc:  # pragma: no cover - passthrough
        raise HTTPException(400, detail=str(exc)) from exc


@app.post("/tasks/{name}/run")
def run_task(
    name: str,
    temporal: bool = False,
    user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
):
    """Execute ``name`` and return its result."""
    sched = get_default_scheduler()
    try:
        if user_id is None:
            raise HTTPException(400, "user_id is required")

        kwargs: dict[str, Any] = {
            "use_temporal": temporal,
            "user_id": user_id,
            "group_id": group_id,
        }
        metadata = sched.run_task_with_metadata(name, **kwargs)
        result = metadata.result
        return {"run_id": metadata.run_id, "result": result}
    except Exception as exc:  # pragma: no cover - passthrough
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/tasks/{name}/run-async")
async def run_task_async(
    name: str,
    temporal: bool = False,
    user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
):
    """Execute ``name`` asynchronously and return its result."""
    sched = get_default_scheduler()
    try:
        if user_id is None:
            raise HTTPException(400, "user_id is required")

        kwargs: dict[str, Any] = {
            "use_temporal": temporal,
            "user_id": user_id,
            "group_id": group_id,
        }
        metadata = sched.run_task_with_metadata(name, **kwargs)
        result = metadata.result
        if inspect.isawaitable(result):
            result = await result
        return {"run_id": metadata.run_id, "result": result}
    except Exception as exc:  # pragma: no cover - passthrough
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/tasks/{name}/schedule")
def schedule_task(
    name: str,
    expression: str,
    user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
):
    """Schedule ``name`` according to ``expression``."""
    sched = get_default_scheduler()
    if not isinstance(sched, CronScheduler):
        raise HTTPException(400, "scheduler lacks cron capabilities")
    task_info = dict(sched._tasks).get(name)
    if not task_info:
        raise HTTPException(404, "unknown task")
    task = task_info["task"]
    try:
        sched.register_task(
            name_or_task=task,
            task_or_expr=expression,
            user_id=user_id,
            group_id=group_id,
        )
        return {"status": "scheduled", "expression": expression}
    except Exception as exc:  # pragma: no cover - passthrough
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/tasks/{name}/disable")
def disable_task(
    name: str,
    user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
):
    """Disable ``name``."""
    sched = get_default_scheduler()
    try:
        if user_id is None:
            raise HTTPException(400, "user_id is required")
        sched.disable_task(name, user_id=user_id, group_id=group_id)
        return {"status": "disabled"}
    except Exception as exc:  # pragma: no cover - passthrough
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/tasks/{name}/pause")
def pause_task(
    name: str,
    job_id: str | None = None,
    user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
):
    """Pause ``name`` so it temporarily stops running."""
    sched = get_default_scheduler()
    try:
        pipeline = None
        if job_id is not None:
            pipeline = get_pipeline(name, run_id=job_id)
            if pipeline is None:
                raise HTTPException(404, "pipeline not running")
        else:
            pipeline = get_pipeline(name)
        if pipeline:
            if user_id is None:
                raise HTTPException(400, "user_id is required")
            pipeline.pause(user_id=user_id, group_id=group_id)
        else:
            if user_id is None:
                raise HTTPException(400, "user_id is required")
            sched.pause_task(name, user_id=user_id, group_id=group_id)
        return {"status": "paused"}
    except Exception as exc:  # pragma: no cover - passthrough
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/tasks/{name}/resume")
def resume_task(
    name: str,
    job_id: str | None = None,
    user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
):
    """Resume a previously paused task."""
    sched = get_default_scheduler()
    try:
        pipeline = None
        if job_id is not None:
            pipeline = get_pipeline(name, run_id=job_id)
            if pipeline is None:
                raise HTTPException(404, "pipeline not running")
        else:
            pipeline = get_pipeline(name)
        if pipeline:
            if user_id is None:
                raise HTTPException(400, "user_id is required")
            pipeline.resume(user_id=user_id, group_id=group_id)
        else:
            if user_id is None:
                raise HTTPException(400, "user_id is required")
            sched.resume_task(name, user_id=user_id, group_id=group_id)
        return {"status": "resumed"}
    except Exception as exc:  # pragma: no cover - passthrough
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/tasks/{job_id}/signal")
def signal_task(
    job_id: str,
    payload: SignalPayload,
    user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
):
    """Deliver a signal to a running pipeline."""

    pipeline = get_pipeline(job_id)
    if pipeline is None:
        raise HTTPException(404, "pipeline not running")

    if payload.kind != "context":
        raise HTTPException(400, "unsupported signal kind")

    consumed = pipeline.attach_context(
        payload.value, user_id=user_id, group_id=group_id
    )
    status_code = 200 if consumed else 202
    body = {
        "status": "delivered" if consumed else "accepted",
        "delivered": consumed,
    }
    return JSONResponse(body, status_code=status_code)


@app.get("/pipeline/{name}")
def pipeline_status(
    name: str,
    user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
    _user_id: str = Depends(get_user_id),
    _group_id: str = Depends(get_group_id),
):
    """Return stored pipeline stage events for ``name``."""
    store = StageStore()
    return store.get_events(
        name,
        user_hash=_ensure_hash(user_id),
        group_id=_ensure_hash(group_id),
    )


@app.get("/pipeline/{name}/audit")
def pipeline_audit(
    name: str,
    user_hash: str | None = None,
    group_filter: str | None = Query(default=None, alias="group_id"),
    user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
    _user_id: str = Depends(get_user_id),
    _caller_group_id: str = Depends(get_group_id),
):
    """Return stored audit events for ``name`` filtered by the optional criteria."""

    store = StageStore()
    scoped_user_hash = _ensure_hash(user_id)
    scoped_group_hash = _ensure_hash(group_id)

    if user_hash is not None:
        requested_user_hash = _ensure_hash(user_hash)
        if requested_user_hash != scoped_user_hash:
            raise HTTPException(403, "cannot access requested user scope")
    else:
        requested_user_hash = scoped_user_hash

    if group_filter is not None:
        requested_group_hash = _ensure_hash(group_filter)
        if requested_group_hash != scoped_group_hash:
            raise HTTPException(403, "cannot access requested group scope")
    else:
        requested_group_hash = scoped_group_hash

    return store.get_events(
        name,
        user_hash=requested_user_hash,
        group_id=requested_group_hash,
        category="audit",
    )


@app.post("/pointers/{name}")
def pointer_add(
    name: str,
    run_id: str,
    user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
):
    """Add a pointer for ``name``."""

    try:
        _pointer_add(name, user_id, run_id, group_id=group_id)
        return {"status": "added"}
    except ValueError as exc:  # pragma: no cover - validation
        raise HTTPException(400, str(exc)) from exc



@app.get("/pointers/{name}")
def pointer_list(
    name: str,
    user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
):
    """List pointers for ``name``."""

    try:
        return _pointer_list(name, user_id=user_id, group_id=group_id)
    except ValueError as exc:  # pragma: no cover - validation
        raise HTTPException(400, str(exc)) from exc


@app.post("/pointers/{name}/receive")
def pointer_receive(
    name: str,
    run_id: str,
    user_hash: str,
    _user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
):
    """Store a received pointer update."""

    _pointer_receive(name, run_id, user_hash, group_id=group_id)

    return {"status": "stored"}


@app.get("/suggestions")
def suggestion_list(
    user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
):
    """Return all generated suggestions."""

    engine = get_default_engine()
    return [
        s.__dict__
        for s in engine.list(user_id=user_id, group_id=group_id)
    ]


@app.post("/suggestions/{suggestion_id}/accept")
def suggestion_accept(
    suggestion_id: str,
    user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
):
    """Accept a suggestion and enqueue its task."""

    engine = get_default_engine()
    if user_id is None:
        raise HTTPException(400, "user_id is required")
    engine.accept(suggestion_id, user_id=user_id, group_id=group_id)
    return {"status": "accepted"}


@app.post("/suggestions/{suggestion_id}/snooze")
def suggestion_snooze(
    suggestion_id: str,
    user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
):
    """Snooze a suggestion."""

    engine = get_default_engine()
    if user_id is None:
        raise HTTPException(400, "user_id is required")
    engine.snooze(suggestion_id, user_id=user_id, group_id=group_id)
    return {"status": "snoozed"}


@app.post("/suggestions/{suggestion_id}/dismiss")
def suggestion_dismiss(
    suggestion_id: str,
    user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
):
    """Dismiss a suggestion."""

    engine = get_default_engine()
    engine.dismiss(suggestion_id, user_id=user_id, group_id=group_id)
    return {"status": "dismissed"}


@app.post("/intent", response_model=IntentResponse)
def intent_route(
    req: IntentRequest,
    user_id: str = Depends(get_user_id),
    group_id: str = Depends(get_group_id),
):
    """Return intent analysis for the provided message."""
    message = sanitize_input(req.message)
    ctx = [sanitize_input(c) for c in req.context]
    result = resolve_intent(
        message, ctx, user_id=user_id, group_id=group_id
    )
    return IntentResponse(
        task=result.task,
        arguments=result.arguments,
        confidence=result.confidence,
        clarification=result.requires_clarification,
    )


__all__ = [
    "app",
    "list_tasks",
    "register_task",
    "run_task",
    "run_task_async",
    "schedule_task",
    "disable_task",
    "pause_task",
    "resume_task",
    "pipeline_status",
    "pointer_add",
    "pointer_list",
    "pointer_receive",
    "suggestion_list",
    "suggestion_accept",
    "suggestion_snooze",
    "suggestion_dismiss",
    "intent_route",
]
