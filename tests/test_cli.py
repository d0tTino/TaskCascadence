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
