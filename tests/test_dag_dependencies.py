from task_cascadence.scheduler.dag import DagCronScheduler
from task_cascadence.plugins import CronTask
import pytest
import yaml


class TaskA(CronTask):
    def __init__(self, log):
        self.log = log

    def run(self):
        self.log.append("A")


class TaskB(CronTask):
    def __init__(self, log):
        self.log = log

    def run(self):
        self.log.append("B")


class TaskC(CronTask):
    def __init__(self, log):
        self.log = log

    def run(self):
        self.log.append("C")


def test_dependency_deduplication(tmp_path, monkeypatch):
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda *a, **k: None)
    log = []
    sched = DagCronScheduler(timezone="UTC", storage_path=tmp_path / "s.yml")
    a = TaskA(log)
    b = TaskB(log)
    c = TaskC(log)
    sched.register_task(c, "* * * * *")
    sched.register_task(b, "* * * * *", dependencies=["TaskC"])
    sched.register_task(a, "* * * * *", dependencies=["TaskB", "TaskC"])

    sched.run_task("TaskA")

    assert log == ["C", "B", "A"]


def test_register_by_name_and_unschedule(tmp_path, monkeypatch):
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda *a, **k: None)
    log = []
    sched = DagCronScheduler(timezone="UTC", storage_path=tmp_path / "s.yml")
    t = TaskA(log)
    sched.register_task("custom", t, dependencies=["TaskA"], user_id="u1")
    assert sched.dependencies["custom"] == ["TaskA"]
    assert "custom" not in sched.schedules
    with pytest.raises(ValueError):
        sched.unschedule("custom")


def test_wrap_task_paused(tmp_path, monkeypatch):
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda *a, **k: None)
    log = []
    sched = DagCronScheduler(timezone="UTC", storage_path=tmp_path / "s.yml")
    t = TaskA(log)
    sched.register_task(t, "* * * * *")
    sched.pause_task("TaskA")
    runner = sched._wrap_task(t)
    runner()
    assert log == []
    sched.resume_task("TaskA")
    runner()
    assert log == ["A"]


def test_restore_skips_unknown_tasks(tmp_path):
    data = {"TaskA": "* * * * *", "Missing": "* * * * *"}
    path = tmp_path / "s.yml"
    yaml.safe_dump(data, path.open("w"))
    t = TaskA([])
    sched = DagCronScheduler(timezone="UTC", storage_path=path, tasks={"TaskA": t})
    assert "Missing" not in sched.dependencies
