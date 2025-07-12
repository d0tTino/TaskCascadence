# Cascadence

Cascadence aims to provide a flexible framework for orchestrating complex, multi-step tasks. The project is inspired by the Product Requirements Document (PRD), which outlines features such as:

- Graph-based task definitions for clear dependency management.
- Plugin architecture allowing custom processors and extensions.
- Configurable execution engine supporting synchronous and asynchronous flows.
- Built-in instrumentation and monitoring.

This repository lays the groundwork for the Python package implementation.

## Webhook Server Example

Cascadence includes a lightweight FastAPI server for handling GitHub and
Cal.com webhook events. Subclass `WebhookTask` to react to incoming events and
start the server using `start_server`:

```python
from task_cascadence.plugins import WebhookTask
from task_cascadence.webhook import start_server


class PrintTask(WebhookTask):
    def handle_event(self, source, event_type, payload):
        print(f"{source} event {event_type}: {payload}")


if __name__ == "__main__":
    start_server()
```

Send GitHub events to `/webhook/github` and Cal.com events to `/webhook/calcom`.
