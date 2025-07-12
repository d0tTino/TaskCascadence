from task_cascadence.scheduler import BaseScheduler
from task_cascadence.plugins import CronTask
from task_cascadence.temporal import TemporalBackend


class DummyTask(CronTask):
    pass


def test_run_task_via_temporal(monkeypatch):
    backend = TemporalBackend()
    scheduler = BaseScheduler(temporal=backend)
    task = DummyTask()
    scheduler.register_task("dummy", task)

    called = {}

    def fake_run(workflow):
        called["workflow"] = workflow
        return "remote"

    monkeypatch.setattr(backend, "run_workflow_sync", fake_run)

    result = scheduler.run_task("dummy")
    assert result == "remote"
    assert called["workflow"] == "DummyTask"


def test_replay_history(monkeypatch):
    backend = TemporalBackend()
    scheduler = BaseScheduler(temporal=backend)

    called = {}

    def fake_replay(path):
        called["path"] = path

    monkeypatch.setattr(backend, "replay", fake_replay)
    scheduler.replay_history("file.json")
    assert called["path"] == "file.json"
