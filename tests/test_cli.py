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
