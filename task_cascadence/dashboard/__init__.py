from __future__ import annotations

from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse

from ..scheduler import get_default_scheduler
from ..stage_store import StageStore
from ..pointer_store import PointerStore
from ..idea_store import IdeaStore

app = FastAPI()


def _get_event(store: StageStore, task_name: str) -> dict | None:
    events = store.get_events(task_name)
    return events[-1] if events else None


def _get_last_status(store: StageStore, task_name: str) -> str | None:
    events = store.get_events(task_name)
    for event in reversed(events):
        stage = event.get("stage")
        if stage in ("finish", "error"):
            return stage
    return None


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request) -> HTMLResponse:
    store = StageStore()
    pointers = PointerStore()
    ideas = IdeaStore()

    sched = get_default_scheduler()
    rows: list[str] = []
    queued: list[str] = []
    for name, info in sched._tasks.items():
        event = _get_event(store, name)
        stage = event["stage"] if event else None
        ts = event.get("time") if event else None
        pointer_count = len(pointers.get_pointers(name))
        if pointer_count:
            queued.append(name)
        paused = info.get("paused", False)
        mode = "async" if sched.is_async(name) else "sync"
        button = (
            f"<form method='post' action='/resume/{name}'>"
            "<button type='submit'>Resume</button></form>"
            if paused
            else (
                f"<form method='post' action='/pause/{name}'>"
                "<button type='submit'>Pause</button></form>"
            )
        )
        status = "paused" if paused else "running"
        last_status = _get_last_status(store, name) or ""
        rows.append(
            "<tr>"
            f"<td>{name}</td>"
            f"<td>{stage or ''}</td>"
            f"<td>{ts or ''}</td>"
            f"<td>{status}</td>"
            f"<td>{mode}</td>"
            f"<td>{pointer_count or ''}</td>"
            f"<td>{last_status}</td>"
            f"<td>{button}</td>"
            "</tr>"
            ""
        )

    queued_items = "\n".join(f"<li>{q}</li>" for q in queued) or "<li>None</li>"
    seed_items = (
        "\n".join(
            f"<li>{s['text']}" \
            f"<form method='post' action='/promote'>" \
            f"<input type='hidden' name='text' value='{s['text']}'>" \
            "<button type='submit'>Promote</button></form></li>"
            for s in ideas.get_seeds()
        )
        or "<li>None</li>"
    )
    body = """
    <html><body>
    <h1>Cascadence Dashboard</h1>
    <h2>Queued Tasks</h2>
    <ul>{queued}</ul>
    <table>
    <tr><th>Task</th><th>Stage</th><th>Time</th><th>Status</th><th>Mode</th><th>Pointers</th>
    <th>Last Run</th><th>Control</th></tr>
    {rows}
    </table>
    <h2>Idea Seeds</h2>
    <ul>{seeds}</ul>
    </body></html>
    """.format(rows="\n".join(rows), queued=queued_items, seeds=seed_items)
    return HTMLResponse(body)


@app.post("/pause/{name}")
def pause(name: str) -> RedirectResponse:
    sched = get_default_scheduler()
    sched.pause_task(name)
    return RedirectResponse("/", status_code=303)


@app.post("/resume/{name}")
def resume(name: str) -> RedirectResponse:
    sched = get_default_scheduler()
    sched.resume_task(name)
    return RedirectResponse("/", status_code=303)


@app.post("/promote")
def promote(text: str = Form(...)) -> RedirectResponse:
    # placeholder action to promote an idea into a task
    return RedirectResponse("/", status_code=303)


__all__ = ["app", "dashboard", "pause", "resume", "promote"]
