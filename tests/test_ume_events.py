import yaml
from typer.testing import CliRunner

from task_cascadence.orchestrator import TaskPipeline
from task_cascadence.cli import app
from task_cascadence.scheduler import BaseScheduler
from task_cascadence.plugins import ExampleTask
from task_cascadence.ume import _hash_user_id


class DemoTask:
    def intake(self):
        pass

    def plan(self):
        return None

    def run(self):
        return "ok"

    def verify(self, result):
        return result


def test_pipeline_stage_events(monkeypatch, tmp_path):
    path = tmp_path / "stages.yml"
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(path))
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")

    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None
    )
    import task_cascadence.ume as ume
    ume._stage_store = None

    pipeline = TaskPipeline(DemoTask())
    pipeline.run(user_id="alice")

    data = yaml.safe_load(path.read_text())
    events = data["DemoTask"]
    stages = [e["stage"] for e in events]
    assert stages == ["intake", "planning", "run", "verification"]
    for e in events:
        assert e["user_hash"] == _hash_user_id("alice")


def test_cli_stage_events(monkeypatch, tmp_path):
    path = tmp_path / "stages.yml"
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(path))
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")

    sched = BaseScheduler()
    sched.register_task("example", ExampleTask())
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)

    monkeypatch.setattr(
        "task_cascadence.ume.emit_task_run", lambda *a, **k: None
    )
    import task_cascadence.ume as ume
    ume._stage_store = None

    runner = CliRunner()
    result = runner.invoke(app, ["run", "example", "--user-id", "bob"])
    assert result.exit_code == 0

    data = yaml.safe_load(path.read_text())
    events = data["example"]
    assert events[0]["stage"] == "start"
    assert events[-1]["stage"] == "finish"
    assert events[0]["user_hash"] == _hash_user_id("bob")
