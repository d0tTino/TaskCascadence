import time

from task_cascadence import ume


def test_ume_emission_order(monkeypatch):
    calls = []

    def record_spec(spec):
        calls.append(("spec", time.time()))

    def record_run(run):
        calls.append(("run", time.time()))

    monkeypatch.setattr(ume, "emit_task_spec", record_spec)
    monkeypatch.setattr(ume, "emit_task_run", record_run)

    ume.emit_task_spec({"name": "test"})
    time.sleep(0.01)
    ume.emit_task_run({"id": 1})

    types = [c[0] for c in calls]
    assert types == ["spec", "run"]
