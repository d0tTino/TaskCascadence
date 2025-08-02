import yaml
import pytest

from task_cascadence.scheduler.dag import DagCronScheduler
from task_cascadence.plugins import CronTask


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


def test_run_respects_dependencies(tmp_path, monkeypatch):
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda run, user_id=None: None)
    log: list[str] = []
    sched = DagCronScheduler(timezone="UTC", storage_path=tmp_path / "s.yml")

    a = TaskA(log)
    b = TaskB(log)
    c = TaskC(log)

    sched.register_task(c, "* * * * *")
    sched.register_task(b, "* * * * *", dependencies=["TaskC"])
    sched.register_task(a, "* * * * *", dependencies=["TaskB", "TaskC"])

    sched.run_task("TaskA")

    assert log == ["C", "B", "A"]


def test_cycle_detection(tmp_path, monkeypatch):
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda run, user_id=None: None)
    log: list[str] = []
    sched = DagCronScheduler(timezone="UTC", storage_path=tmp_path / "s.yml")

    a = TaskA(log)
    b = TaskB(log)

    sched.register_task(a, "* * * * *", dependencies=["TaskB"])
    sched.register_task(b, "* * * * *", dependencies=["TaskA"])

    with pytest.raises(ValueError):
        sched.run_task("TaskA")


def test_restore_dag_from_file(tmp_path, monkeypatch):
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda run, user_id=None: None)
    log: list[str] = []
    sched = DagCronScheduler(timezone="UTC", storage_path=tmp_path / "s.yml")

    c = TaskC(log)
    b = TaskB(log)
    a = TaskA(log)

    sched.register_task(c, "*/5 * * * *")
    sched.register_task(b, "*/5 * * * *", dependencies=["TaskC"])
    sched.register_task(a, "*/5 * * * *", dependencies=["TaskB"])

    data = yaml.safe_load((tmp_path / "s.yml").read_text())
    assert data["TaskA"]["deps"] == ["TaskB"]

    log.clear()
    sched2 = DagCronScheduler(
        timezone="UTC",
        storage_path=tmp_path / "s.yml",
        tasks={"TaskA": a, "TaskB": b, "TaskC": c},
    )
    job = sched2.scheduler.get_job("TaskA")
    assert job is not None
    job.func()
    assert log == ["C", "B", "A"]
