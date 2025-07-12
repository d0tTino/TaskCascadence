from task_cascadence.plugins import WebhookTask


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
