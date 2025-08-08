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

    sched.register_task(c, "* * * * *", user_id="alice")
    sched.register_task(b, "* * * * *", dependencies=["TaskC"], user_id="alice")
    sched.register_task(a, "* * * * *", dependencies=["TaskB", "TaskC"], user_id="alice")

    sched.run_task("TaskA", user_id="alice")

    assert log == ["C", "B", "A"]


def test_cycle_detection(tmp_path, monkeypatch):
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda run, user_id=None: None)
    log: list[str] = []
    sched = DagCronScheduler(timezone="UTC", storage_path=tmp_path / "s.yml")

    a = TaskA(log)
    b = TaskB(log)

    sched.register_task(a, "* * * * *", dependencies=["TaskB"], user_id="alice")
    sched.register_task(b, "* * * * *", dependencies=["TaskA"], user_id="alice")

    with pytest.raises(ValueError):
        sched.run_task("TaskA", user_id="alice")


def test_restore_dag_from_file(tmp_path, monkeypatch):
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda run, user_id=None: None)
    log: list[str] = []
    sched = DagCronScheduler(timezone="UTC", storage_path=tmp_path / "s.yml")

    c = TaskC(log)
    b = TaskB(log)
    a = TaskA(log)

    sched.register_task(c, "*/5 * * * *", user_id="alice")
    sched.register_task(b, "*/5 * * * *", dependencies=["TaskC"], user_id="alice")
    sched.register_task(a, "*/5 * * * *", dependencies=["TaskB"], user_id="alice")

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
