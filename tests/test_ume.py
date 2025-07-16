import time

from datetime import datetime

from task_cascadence import ume
from task_cascadence.ume.models import TaskRun, TaskSpec


def test_ume_emission_order(monkeypatch):
    calls = []

    def record_spec(spec):
        calls.append(("spec", time.time()))

    def record_run(run):
        calls.append(("run", time.time()))

    monkeypatch.setattr(ume, "emit_task_spec", record_spec)
    monkeypatch.setattr(ume, "emit_task_run", record_run)

    spec = TaskSpec(id="1", name="test")
    ume.emit_task_spec(spec)
    time.sleep(0.01)
    run = TaskRun(
        spec=spec,
        run_id="run1",
        status="success",
        started_at=datetime.now(),
        finished_at=datetime.now(),
    )
    ume.emit_task_run(run)

    types = [c[0] for c in calls]
    assert types == ["spec", "run"]
