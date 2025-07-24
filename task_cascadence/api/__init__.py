from __future__ import annotations

from fastapi import FastAPI, Depends, HTTPException, Header

from ..scheduler import get_default_scheduler, CronScheduler
from ..stage_store import StageStore
from ..pointer_store import PointerStore
from ..plugins import PointerTask
from ..ume import emit_pointer_update, _hash_user_id
from ..ume.models import PointerUpdate

app = FastAPI()


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
        sched.pause_task(name)
        return {"status": "paused"}
    except Exception as exc:  # pragma: no cover - passthrough
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/tasks/{name}/resume")
def resume_task(name: str):
    """Resume a previously paused task."""
    sched = get_default_scheduler()
    try:
        sched.resume_task(name)
        return {"status": "resumed"}
    except Exception as exc:  # pragma: no cover - passthrough
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/pipeline/{name}")
def pipeline_status(name: str):
    """Return stored pipeline stage events for ``name``."""
    store = StageStore()
    return store.get_events(name)


@app.post("/pointers/{name}/add")
def pointer_add(name: str, user_id: str, run_id: str) -> dict[str, str]:
    """Add a pointer for ``name``."""

    sched = get_default_scheduler()
    task_info = dict(sched._tasks).get(name)
    if not task_info or not isinstance(task_info["task"], PointerTask):
        raise HTTPException(400, "unknown pointer task")

    task: PointerTask = task_info["task"]
    task.add_pointer(user_id, run_id)
    return {"status": "pointer added"}


@app.get("/pointers/{name}")
def pointer_list(name: str):
    """List stored pointers for ``name``."""

    store = PointerStore()
    return store.get_pointers(name)


@app.post("/pointers/{name}/send")
def pointer_send(name: str, user_id: str, run_id: str) -> dict[str, str]:
    """Publish a pointer update."""

    update = PointerUpdate(
        task_name=name,
        run_id=run_id,
        user_hash=_hash_user_id(user_id),
    )
    emit_pointer_update(update)
    return {"status": "sent"}


@app.post("/pointers/{name}/receive")
def pointer_receive(name: str, run_id: str, user_hash: str) -> dict[str, str]:
    """Store a received pointer update."""

    store = PointerStore()
    store.apply_update(PointerUpdate(task_name=name, run_id=run_id, user_hash=user_hash))
    return {"status": "stored"}


__all__ = [
    "app",
    "list_tasks",
    "run_task",
    "schedule_task",
    "disable_task",
    "pause_task",
    "resume_task",
    "pipeline_status",
    "pointer_add",
    "pointer_list",
    "pointer_send",
    "pointer_receive",
]
