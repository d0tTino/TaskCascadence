from typer.testing import CliRunner

from task_cascadence.cli import app
from task_cascadence.scheduler import BaseScheduler, CronScheduler
from task_cascadence.plugins import ExampleTask, CronTask


def test_cli_pause_resume(monkeypatch):
    sched = BaseScheduler()
    task = ExampleTask()
    sched.register_task("example", task)
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)

    runner = CliRunner()
    result = runner.invoke(app, ["pause", "example"])
    assert result.exit_code == 0
    assert sched._tasks["example"]["paused"] is True

    result = runner.invoke(app, ["resume", "example"])
    assert result.exit_code == 0
    assert sched._tasks["example"]["paused"] is False


class DummyTask(CronTask):
    def __init__(self):
        self.count = 0

    def run(self):
        self.count += 1


def test_paused_job_does_not_run(monkeypatch, tmp_path):
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "s.yml")
    task = DummyTask()
    sched.register_task(name_or_task=task, task_or_expr="* * * * *")
    sched.pause_task("DummyTask")

    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda run, user_id=None: None)

    job = sched.scheduler.get_job("DummyTask")
    job.func()

    assert task.count == 0
