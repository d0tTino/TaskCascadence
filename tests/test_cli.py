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


def test_metrics_option_starts_server(monkeypatch):
    called = {}

    def fake_start(port: int):
        called["port"] = port

    import task_cascadence.cli as cli
    monkeypatch.setattr(cli, "start_metrics_server", fake_start)

    runner = CliRunner()
    result = runner.invoke(app, ["--metrics-port", "9100", "list"])

    assert result.exit_code == 0
    assert called["port"] == 9100

