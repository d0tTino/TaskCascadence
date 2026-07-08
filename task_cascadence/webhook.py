from __future__ import annotations

import hashlib
import hmac
import inspect
import json
import os
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, HTTPException, Request
import uvicorn

from .plugins import WebhookTask, webhook_task_instances
from .ume import emit_audit_log, emit_stage_update_event

app = FastAPI()


@dataclass(frozen=True)
class WebhookScope:
    """Resolved request scope for a webhook delivery."""

    user_id: str
    group_id: str
    source: str


def _get_task_instance(task_cls: type[WebhookTask]) -> WebhookTask:
    """Return the singleton instance for ``task_cls``."""

    return webhook_task_instances.setdefault(task_cls, task_cls())


def _configured_scope(source: str) -> dict[str, str] | None:
    """Return configured secret-to-scope mapping for ``source`` if present."""

    raw = os.environ.get("CASCADENCE_WEBHOOK_SCOPES", "").strip()
    if not raw:
        return None
    data = json.loads(raw)
    scope = data.get(source)
    if not isinstance(scope, dict):
        return None
    return {str(key): str(value) for key, value in scope.items()}


def _verify_github_signature(request: Request, body: bytes, secret: str) -> bool:
    signature = request.headers.get("X-Hub-Signature-256", "")
    if not signature.startswith("sha256="):
        return False
    digest = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(signature, f"sha256={digest}")


def _verify_shared_secret(request: Request, secret: str) -> bool:
    supplied = request.headers.get("X-Webhook-Secret", "")
    return bool(supplied) and hmac.compare_digest(supplied, secret)


def resolve_webhook_scope(source: str, request: Request, body: bytes) -> WebhookScope:
    """Resolve authenticated user/group context for a webhook request.

    Requests may either provide explicit ``X-User-ID`` and ``X-Group-ID``
    headers, or match a configured provider secret in
    ``CASCADENCE_WEBHOOK_SCOPES``. Secret mappings use the shape
    ``{"github": {"secret": "...", "user_id": "...", "group_id": "..."}}``.
    """

    user_id = request.headers.get("X-User-ID")
    group_id = request.headers.get("X-Group-ID")
    if user_id and group_id:
        return WebhookScope(user_id=user_id, group_id=group_id, source="headers")
    if user_id or group_id:
        raise HTTPException(
            status_code=400,
            detail="webhook requests require both X-User-ID and X-Group-ID",
        )

    configured = _configured_scope(source)
    if configured is None:
        raise HTTPException(
            status_code=400,
            detail=(
                "webhook request could not be scoped; provide X-User-ID/X-Group-ID "
                "or configure a verified secret mapping"
            ),
        )

    secret = configured.get("secret")
    mapped_user = configured.get("user_id")
    mapped_group = configured.get("group_id")
    if not secret or not mapped_user or not mapped_group:
        raise HTTPException(status_code=400, detail="webhook secret mapping is incomplete")

    verified = _verify_shared_secret(request, secret)
    if source == "github":
        verified = verified or _verify_github_signature(request, body, secret)
    if not verified:
        raise HTTPException(status_code=401, detail="invalid webhook signature or secret")

    return WebhookScope(user_id=mapped_user, group_id=mapped_group, source="secret")


def _deliver_event(
    source: str,
    event_type: str,
    payload: dict[str, Any],
    scope: WebhookScope,
) -> None:
    for task in webhook_task_instances.values():
        handle_event = getattr(task, "handle_event", None)
        if handle_event is None:
            continue
        params = inspect.signature(handle_event).parameters
        if "user_id" in params or "group_id" in params:
            handle_event(
                source,
                event_type,
                payload,
                user_id=scope.user_id,
                group_id=scope.group_id,
            )
        else:
            handle_event(source, event_type, payload)
        emit_stage_update_event(
            task.name,
            f"webhook.{source}.{event_type}",
            user_id=scope.user_id,
            group_id=scope.group_id,
        )
        emit_audit_log(
            task.name,
            "webhook",
            "received",
            user_id=scope.user_id,
            group_id=scope.group_id,
            reason=f"{source}:{event_type}",
        )


async def _payload_and_scope(source: str, request: Request) -> tuple[dict[str, Any], WebhookScope]:
    body = await request.body()
    scope = resolve_webhook_scope(source, request, body)
    try:
        payload = json.loads(body or b"{}")
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="webhook payload must be valid JSON") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="webhook payload must be a JSON object")
    return payload, scope


@app.post("/webhook/github")
async def github_webhook(request: Request):
    payload, scope = await _payload_and_scope("github", request)
    event_type = request.headers.get("X-GitHub-Event", "")
    _deliver_event("github", event_type, payload, scope)
    return {"status": "received"}


@app.post("/webhook/calcom")
async def calcom_webhook(request: Request):
    payload, scope = await _payload_and_scope("calcom", request)
    event_type = request.headers.get("Cal-Event-Type", "")
    _deliver_event("calcom", event_type, payload, scope)
    return {"status": "received"}


def start_server(host: str = "0.0.0.0", port: int = 8000):
    """Start the FastAPI webhook server."""
    uvicorn.run(app, host=host, port=port)
