from click.exceptions import UsageError
import pytest
from typer.testing import CliRunner

from task_cascadence.cli import app, main


def test_cli_main_returns_none():
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    with pytest.raises(UsageError):
        main([])


def test_cli_list_shows_example_task():
    runner = CliRunner()
    result = runner.invoke(app, ["list"])
    assert result.exit_code == 0
    assert "example\tenabled" in result.output


def test_cli_run_executes_task(monkeypatch):
    from task_cascadence.plugins import ExampleTask

    called = []

    def fake_run(self):
        called.append(True)

    monkeypatch.setattr(ExampleTask, "run", fake_run)
    runner = CliRunner()
    result = runner.invoke(app, ["run", "example"])
    assert result.exit_code == 0
    assert called == [True]


def test_cli_disable_disables_task():
    runner = CliRunner()
    result = runner.invoke(app, ["disable", "example"])
    assert result.exit_code == 0
    assert "example disabled" in result.output

    from task_cascadence.scheduler import default_scheduler

    assert default_scheduler._tasks["example"]["disabled"] is True

