import json
from typer.testing import CliRunner

from task_cascadence.cli import app
from task_cascadence.scheduler import get_default_scheduler
from task_cascadence.n8n import to_workflow
from task_cascadence import initialize


def test_to_workflow_produces_nodes():
    initialize()
    wf = to_workflow(get_default_scheduler())
    assert "nodes" in wf
    assert any(n["name"] == "example" for n in wf["nodes"])


def test_cli_export_n8n(tmp_path):
    initialize()
    out = tmp_path / "flow.json"
    runner = CliRunner()
    result = runner.invoke(app, ["export-n8n", str(out)])
    assert result.exit_code == 0
    data = json.loads(out.read_text())
    assert "nodes" in data
