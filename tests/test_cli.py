from typer.testing import CliRunner
from task_cascadence.cli import app


def test_cli_list_runs():
    runner = CliRunner()
    result = runner.invoke(app, ["list"])
    assert result.exit_code == 0
