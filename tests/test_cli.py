from click.exceptions import UsageError
import pytest
from typer.testing import CliRunner

from task_cascadence.cli import app, main
from task_cascadence.plugins import ManualTrigger
from task_cascadence.scheduler import default_scheduler


def test_cli_main_returns_none():
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    with pytest.raises(UsageError):
        main([])


class ManualTask(ManualTrigger):
    name = "manual_demo"

    def run(self):
        return "ok"


def test_manual_trigger_cli(monkeypatch):
    default_scheduler.register_task("manual_demo", ManualTask())

    from task_cascadence import ume

    monkeypatch.setattr(ume, "emit_task_run", lambda run: None)

    runner = CliRunner()
    result = runner.invoke(app, ["trigger", "manual_demo"])
    assert result.exit_code == 0


def test_webhook_command_runs_uvicorn(monkeypatch):
    called = {}

    def fake_run(app, host="0.0.0.0", port=8000):
        called["host"] = host
        called["port"] = port

    monkeypatch.setattr("task_cascadence.webhook.uvicorn.run", fake_run)

    runner = CliRunner()
    result = runner.invoke(app, ["webhook", "--host", "127.0.0.1", "--port", "9000"])

    assert result.exit_code == 0
    assert called == {"host": "127.0.0.1", "port": 9000}


def test_cli_schedule_creates_entry(monkeypatch, tmp_path):
    from task_cascadence.scheduler import CronScheduler
    from task_cascadence.plugins import ExampleTask
    import yaml

    sched = CronScheduler(storage_path=tmp_path / "sched.yml")
    monkeypatch.setattr("task_cascadence.cli.default_scheduler", sched)
    sched.register_task("example", ExampleTask())

    runner = CliRunner()
    result = runner.invoke(app, ["schedule", "example", "0 12 * * *"])

    assert result.exit_code == 0
    data = yaml.safe_load((tmp_path / "sched.yml").read_text())
    assert data["ExampleTask"] == "0 12 * * *"


def test_cli_schedule_unknown_task(monkeypatch):
    from task_cascadence.scheduler import CronScheduler

    sched = CronScheduler(storage_path="/tmp/dummy.yml")
    monkeypatch.setattr("task_cascadence.cli.default_scheduler", sched)

    runner = CliRunner()
    result = runner.invoke(app, ["schedule", "missing", "* * * * *"])

    assert result.exit_code == 1

