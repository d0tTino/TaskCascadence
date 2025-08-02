from fastapi import FastAPI, Request
import uvicorn

from .plugins import WebhookTask, webhook_task_instances

app = FastAPI()


def _get_task_instance(task_cls: type[WebhookTask]) -> WebhookTask:
    """Return the singleton instance for ``task_cls``."""

    return webhook_task_instances.setdefault(task_cls, task_cls())


@app.post("/webhook/github")
async def github_webhook(request: Request):
    payload = await request.json()
    event_type = request.headers.get("X-GitHub-Event", "")
    for task in webhook_task_instances.values():
        if hasattr(task, "handle_event"):
            task.handle_event("github", event_type, payload)
    return {"status": "received"}


@app.post("/webhook/calcom")
async def calcom_webhook(request: Request):
    payload = await request.json()
    event_type = request.headers.get("Cal-Event-Type", "")
    for task in webhook_task_instances.values():
        if hasattr(task, "handle_event"):
            task.handle_event("calcom", event_type, payload)
    return {"status": "received"}


def start_server(host: str = "0.0.0.0", port: int = 8000):
    """Start the FastAPI webhook server."""
    uvicorn.run(app, host=host, port=port)
