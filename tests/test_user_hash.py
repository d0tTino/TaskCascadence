from datetime import datetime

from task_cascadence.ume import emit_task_spec, emit_task_run
from task_cascadence.ume.models import TaskSpec, TaskRun


class Collector:
    def __init__(self):
        self.events = []

    def enqueue(self, obj):
        self.events.append(obj)

def test_spec_user_hashes_unique():
    client = Collector()
    spec1 = TaskSpec(id="1", name="a")
    spec2 = TaskSpec(id="2", name="b")
    emit_task_spec(spec1, client, user_id="alice")
    emit_task_spec(spec2, client, user_id="bob")
    h1 = client.events[0].user_hash
    h2 = client.events[1].user_hash
    assert h1 != h2
    assert h1 != "alice"
    assert h2 != "bob"

def test_run_user_hash_not_raw():
    client = Collector()
    spec = TaskSpec(id="3", name="c")
    run = TaskRun(
        spec=spec,
        run_id="r1",
        status="ok",
        started_at=datetime.now(),
        finished_at=datetime.now(),
    )
    emit_task_run(run, client, user_id="alice")
    assert client.events[0].user_hash != "alice"

