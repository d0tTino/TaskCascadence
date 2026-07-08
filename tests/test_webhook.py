import hashlib
import hmac
import json
from typing import Any

from fastapi.testclient import TestClient

from task_cascadence.plugins import (
    WebhookTask,
    register_webhook_task,
    webhook_task_registry,
)
from task_cascadence.ume import _hash_user_id
from task_cascadence.webhook import app, webhook_task_instances


class DummyWebhookTask(WebhookTask):
    def __init__(self):
        self.events = []

    def handle_event(self, event):
        self.events.append(event)
        return f"handled {event}"


def test_webhook_routing():
    task = DummyWebhookTask()
    result = task.handle_event({"type": "ping"})

    assert result == "handled {'type': 'ping'}"
    assert task.events == [{"type": "ping"}]


def test_registered_task_receives_event_with_header_scope(monkeypatch):
    webhook_task_registry.clear()
    webhook_task_instances.clear()
    stage_calls = []
    audit_calls = []
    monkeypatch.setattr(
        "task_cascadence.webhook.emit_stage_update_event",
        lambda *args, **kwargs: stage_calls.append((args, kwargs)),
    )
    monkeypatch.setattr(
        "task_cascadence.webhook.emit_audit_log",
        lambda *args, **kwargs: audit_calls.append((args, kwargs)),
    )

    @register_webhook_task
    class CollectorTask(WebhookTask):
        name = "collector"

        def __init__(self):
            self.events: list[tuple[str, str, dict[str, Any], str, str]] = []

        def handle_event(self, source, event_type, payload, *, user_id, group_id):
            self.events.append((source, event_type, payload, user_id, group_id))

    client = TestClient(app)
    payload = {"action": "opened"}
    headers = {
        "X-GitHub-Event": "issues",
        "X-User-ID": "alice",
        "X-Group-ID": "engineering",
    }

    response = client.post("/webhook/github", json=payload, headers=headers)
    response = client.post("/webhook/github", json=payload, headers=headers)

    assert response.json() == {"status": "received"}
    task = webhook_task_instances[CollectorTask]
    assert task.events == [
        ("github", "issues", payload, "alice", "engineering"),
        ("github", "issues", payload, "alice", "engineering"),
    ]
    assert stage_calls[-1][1]["user_id"] == "alice"
    assert audit_calls[-1][1]["group_id"] == "engineering"


def test_calcom_task_receives_event_with_secret_scope(monkeypatch):
    """Webhook tasks should receive Cal.com events."""

    webhook_task_registry.clear()
    webhook_task_instances.clear()
    monkeypatch.setenv(
        "CASCADENCE_WEBHOOK_SCOPES",
        json.dumps(
            {
                "calcom": {
                    "secret": "cal-secret",
                    "user_id": "cal-user",
                    "group_id": "cal-group",
                }
            }
        ),
    )
    monkeypatch.setattr("task_cascadence.webhook.emit_stage_update_event", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.webhook.emit_audit_log", lambda *a, **k: None)

    @register_webhook_task
    class CollectorTask(WebhookTask):
        def __init__(self):
            self.events: list[tuple[str, str, dict[str, Any], str, str]] = []

        def handle_event(self, source, event_type, payload, *, user_id, group_id):
            self.events.append((source, event_type, payload, user_id, group_id))

    client = TestClient(app)
    payload = {"event": "created"}
    headers = {"Cal-Event-Type": "booking", "X-Webhook-Secret": "cal-secret"}

    response = client.post("/webhook/calcom", json=payload, headers=headers)

    assert response.json() == {"status": "received"}
    task = webhook_task_instances[CollectorTask]
    assert task.events == [("calcom", "booking", payload, "cal-user", "cal-group")]


def test_webhook_rejects_unscoped_request():
    client = TestClient(app)

    response = client.post("/webhook/github", json={"action": "opened"})

    assert response.status_code == 400


def test_webhook_rejects_invalid_secret(monkeypatch):
    monkeypatch.setenv(
        "CASCADENCE_WEBHOOK_SCOPES",
        json.dumps(
            {"github": {"secret": "secret", "user_id": "bot", "group_id": "ops"}}
        ),
    )
    client = TestClient(app)

    response = client.post(
        "/webhook/github",
        json={"action": "opened"},
        headers={"X-Webhook-Secret": "wrong"},
    )

    assert response.status_code == 401


def test_github_signature_maps_scope_and_hashes_audit(monkeypatch):
    webhook_task_registry.clear()
    webhook_task_instances.clear()
    audit_calls = []
    monkeypatch.setenv(
        "CASCADENCE_WEBHOOK_SCOPES",
        json.dumps(
            {"github": {"secret": "secret", "user_id": "bot", "group_id": "ops"}}
        ),
    )
    monkeypatch.setattr("task_cascadence.webhook.emit_stage_update_event", lambda *a, **k: None)
    monkeypatch.setattr(
        "task_cascadence.webhook.emit_audit_log",
        lambda *args, **kwargs: audit_calls.append((args, kwargs)),
    )

    @register_webhook_task
    class CollectorTask(WebhookTask):
        name = "collector"

        def handle_event(self, source, event_type, payload, *, user_id, group_id):
            assert user_id == "bot"
            assert group_id == "ops"

    body = json.dumps({"action": "opened"}).encode()
    signature = hmac.new(b"secret", body, hashlib.sha256).hexdigest()
    client = TestClient(app)

    response = client.post(
        "/webhook/github",
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-GitHub-Event": "issues",
            "X-Hub-Signature-256": f"sha256={signature}",
        },
    )

    assert response.status_code == 200
    assert audit_calls[0][1]["user_id"] == "bot"
    assert _hash_user_id(audit_calls[0][1]["user_id"]) != "bot"
