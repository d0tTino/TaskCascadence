import time

from datetime import datetime
from google.protobuf.timestamp_pb2 import Timestamp

from task_cascadence import ume
from task_cascadence.ume.protos.tasks_pb2 import TaskRun, TaskSpec


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
    start_ts = Timestamp()
    start_ts.FromDatetime(datetime.now())
    end_ts = Timestamp()
    end_ts.FromDatetime(datetime.now())
    run = TaskRun(
        spec=spec,
        run_id="run1",
        status="success",
        started_at=start_ts,
        finished_at=end_ts,
    )
    ume.emit_task_run(run)

    types = [c[0] for c in calls]
    assert types == ["spec", "run"]
