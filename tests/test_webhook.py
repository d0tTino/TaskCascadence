from fastapi.testclient import TestClient

from task_cascadence.plugins import (
    WebhookTask,
    register_webhook_task,
    webhook_task_registry,
)
from task_cascadence.webhook import app


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


def test_registered_task_receives_event():
    webhook_task_registry.clear()

    @register_webhook_task
    class CollectorTask(WebhookTask):
        events = []

        def handle_event(self, source, event_type, payload):
            self.__class__.events.append((source, event_type, payload))

    client = TestClient(app)
    payload = {"action": "opened"}
    headers = {"X-GitHub-Event": "issues"}

    response = client.post("/webhook/github", json=payload, headers=headers)

    assert response.json() == {"status": "received"}
    assert CollectorTask.events == [("github", "issues", payload)]


def test_calcom_task_receives_event():
    """Webhook tasks should receive Cal.com events."""

    webhook_task_registry.clear()

    @register_webhook_task
    class CollectorTask(WebhookTask):
        events = []

        def handle_event(self, source, event_type, payload):
            self.__class__.events.append((source, event_type, payload))

    client = TestClient(app)
    payload = {"event": "created"}
    headers = {"Cal-Event-Type": "booking"}

    response = client.post("/webhook/calcom", json=payload, headers=headers)

    assert response.json() == {"status": "received"}
    assert CollectorTask.events == [("calcom", "booking", payload)]
