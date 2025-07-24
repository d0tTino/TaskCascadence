from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from ..scheduler import get_default_scheduler
from ..stage_store import StageStore

app = FastAPI()


def _get_event(store: StageStore, task_name: str) -> dict | None:
    events = store.get_events(task_name)
    return events[-1] if events else None


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request) -> HTMLResponse:
    store = StageStore()
    sched = get_default_scheduler()
    rows = []
    for name, info in sched._tasks.items():
        event = _get_event(store, name)
        stage = event["stage"] if event else None
        ts = event.get("time") if event else None
        paused = info.get("paused", False)
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
        rows.append(
            f"<tr><td>{name}</td><td>{stage or ''}</td><td>{ts or ''}</td><td>{status}</td><td>{button}</td></tr>"
        )
    body = """
    <html><body>
    <h1>Cascadence Dashboard</h1>
    <table>
    <tr><th>Task</th><th>Stage</th><th>Time</th><th>Status</th><th>Control</th></tr>
    {rows}
    </table>
    </body></html>
    """.format(rows="\n".join(rows))
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


__all__ = ["app", "dashboard", "pause", "resume"]
