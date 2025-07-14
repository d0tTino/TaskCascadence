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


def test_manual_trigger_cli():
    default_scheduler.register_task("manual_demo", ManualTask())
    runner = CliRunner()
    result = runner.invoke(app, ["trigger", "manual_demo"])
    assert result.exit_code == 0


def test_metrics_command(monkeypatch):
    called = {}

    def fake_start(port=8000):
        called["port"] = port

    import task_cascadence.cli as cli

    monkeypatch.setattr(cli, "start_metrics_server", fake_start)
    runner = CliRunner()
    result = runner.invoke(app, ["metrics", "--port", "9000"])
    assert result.exit_code == 0
    assert called["port"] == 9000
