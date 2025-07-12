from datetime import datetime
import time


from task_cascadence.ume import emit_task_run, emit_task_spec
from task_cascadence.ume.models import TaskRun, TaskSpec


class MockClient:
    def __init__(self):
        self.events = []

    def enqueue(self, obj):
        self.events.append((obj, time.monotonic()))


def test_emit_task_run_within_deadline():
    client = MockClient()
    spec = TaskSpec(id="1", name="sample")
    run = TaskRun(
        spec=spec,
        run_id="run1",
        status="success",
        started_at=datetime.now(),
        finished_at=datetime.now(),
    )

    start = time.monotonic()
    emit_task_run(run, client)
    assert client.events
    delay = client.events[0][1] - start
    assert delay < 0.2


def test_emit_task_spec_within_deadline():
    client = MockClient()
    spec = TaskSpec(id="2", name="other")
    start = time.monotonic()
    emit_task_spec(spec, client)
    assert client.events
    delay = client.events[0][1] - start
    assert delay < 0.2
