from __future__ import annotations

from fastapi import FastAPI, Depends, HTTPException, Header
import inspect
from pydantic import BaseModel, Field
from typing import Any, Dict

from ..scheduler import get_default_scheduler, CronScheduler
from ..stage_store import StageStore
from ..cli import _pointer_add, _pointer_list, _pointer_receive
from ..pipeline_registry import get_pipeline
from ..plugins import load_plugin
from ..task_store import TaskStore
from ..suggestions.engine import get_default_engine
from ..intent import resolve_intent, sanitize_input

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


def get_user_id(x_user_id: str | None = Header(default=None)) -> str | None:
    """Return the user identifier from ``X-User-ID`` header if supplied."""
    return x_user_id


@app.get("/tasks")
def list_tasks():
    """Return all registered tasks."""
    sched = get_default_scheduler()
    return [
        {"name": name, "disabled": disabled}
        for name, disabled in sched.list_tasks()
    ]


@app.post("/tasks")
def register_task(path: str, schedule: str | None = None):
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
    user_id: str | None = Depends(get_user_id),
):
    """Execute ``name`` and return its result."""
    sched = get_default_scheduler()
    try:
        result = sched.run_task(name, use_temporal=temporal, user_id=user_id)
        return {"result": result}
    except Exception as exc:  # pragma: no cover - passthrough
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/tasks/{name}/run-async")
async def run_task_async(
    name: str,
    temporal: bool = False,
    user_id: str | None = Depends(get_user_id),
):
    """Execute ``name`` asynchronously and return its result."""
    sched = get_default_scheduler()
    try:
        result = sched.run_task(name, use_temporal=temporal, user_id=user_id)
        if inspect.isawaitable(result):
            result = await result
        return {"result": result}
    except Exception as exc:  # pragma: no cover - passthrough
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/tasks/{name}/schedule")
def schedule_task(
    name: str,
    expression: str,
    user_id: str | None = Depends(get_user_id),
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
        sched.register_task(name_or_task=task, task_or_expr=expression, user_id=user_id)
        return {"status": "scheduled", "expression": expression}
    except Exception as exc:  # pragma: no cover - passthrough
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/tasks/{name}/disable")
def disable_task(name: str):
    """Disable ``name``."""
    sched = get_default_scheduler()
    try:
        sched.disable_task(name)
        return {"status": "disabled"}
    except Exception as exc:  # pragma: no cover - passthrough
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/tasks/{name}/pause")
def pause_task(name: str):
    """Pause ``name`` so it temporarily stops running."""
    sched = get_default_scheduler()
    try:
        pipeline = get_pipeline(name)
        if pipeline:
            pipeline.pause()
        else:
            sched.pause_task(name)
        return {"status": "paused"}
    except Exception as exc:  # pragma: no cover - passthrough
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/tasks/{name}/resume")
def resume_task(name: str):
    """Resume a previously paused task."""
    sched = get_default_scheduler()
    try:
        pipeline = get_pipeline(name)
        if pipeline:
            pipeline.resume()
        else:
            sched.resume_task(name)
        return {"status": "resumed"}
    except Exception as exc:  # pragma: no cover - passthrough
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/pipeline/{name}")
def pipeline_status(name: str):
    """Return stored pipeline stage events for ``name``."""
    store = StageStore()
    return store.get_events(name)


@app.post("/pointers/{name}")
def pointer_add(name: str, user_id: str, run_id: str):
    """Add a pointer for ``name``."""

    try:
        _pointer_add(name, user_id, run_id)
        return {"status": "added"}
    except ValueError as exc:  # pragma: no cover - validation
        raise HTTPException(400, str(exc)) from exc



@app.get("/pointers/{name}")
def pointer_list(name: str):
    """List pointers for ``name``."""

    try:
        return _pointer_list(name)
    except ValueError as exc:  # pragma: no cover - validation
        raise HTTPException(400, str(exc)) from exc


@app.post("/pointers/{name}/receive")
def pointer_receive(name: str, run_id: str, user_hash: str):
    """Store a received pointer update."""

    _pointer_receive(name, run_id, user_hash)

    return {"status": "stored"}


@app.get("/suggestions")
def suggestion_list():
    """Return all generated suggestions."""

    engine = get_default_engine()
    return [s.__dict__ for s in engine.list()]


@app.post("/suggestions/{suggestion_id}/accept")
def suggestion_accept(
    suggestion_id: str, user_id: str | None = Depends(get_user_id)
):
    """Accept a suggestion and enqueue its task."""

    engine = get_default_engine()
    engine.accept(suggestion_id, user_id=user_id)
    return {"status": "accepted"}


@app.post("/suggestions/{suggestion_id}/snooze")
def suggestion_snooze(suggestion_id: str):
    """Snooze a suggestion."""

    engine = get_default_engine()
    engine.snooze(suggestion_id)
    return {"status": "snoozed"}


@app.post("/suggestions/{suggestion_id}/dismiss")
def suggestion_dismiss(suggestion_id: str):
    """Dismiss a suggestion."""

    engine = get_default_engine()
    engine.dismiss(suggestion_id)
    return {"status": "dismissed"}


@app.post("/intent", response_model=IntentResponse)
def intent_route(req: IntentRequest):
    """Return intent analysis for the provided message."""
    message = sanitize_input(req.message)
    ctx = [sanitize_input(c) for c in req.context]
    result = resolve_intent(message, ctx)
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
