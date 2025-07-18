from click.exceptions import UsageError
import pytest
from typer.testing import CliRunner
import types
import sys

from task_cascadence.cli import app, main
from task_cascadence.plugins import ManualTrigger, CronTask
from task_cascadence.scheduler import get_default_scheduler
from task_cascadence import initialize
from task_cascadence.temporal import TemporalBackend


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
    initialize()
    sched = get_default_scheduler()
    sched.register_task("manual_demo", ManualTask())

    from task_cascadence import ume

    monkeypatch.setattr(ume, "emit_task_run", lambda run: None)

    runner = CliRunner()
    result = runner.invoke(app, ["trigger", "manual_demo"])
    assert result.exit_code == 0


class DummyTask(CronTask):
    pass


def test_run_command_temporal(monkeypatch):
    backend = TemporalBackend()
    initialize()
    sched = get_default_scheduler()
    sched._temporal = backend
    sched.register_task("dummy", DummyTask())

    called = {}

    def fake_run(workflow):
        called["workflow"] = workflow
        return "remote"

    monkeypatch.setattr(backend, "run_workflow_sync", fake_run)

    runner = CliRunner()
    result = runner.invoke(app, ["run", "dummy", "--temporal"])
    assert result.exit_code == 0
    assert called["workflow"] == "DummyTask"


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
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)
    sched.register_task("example", ExampleTask())

    runner = CliRunner()
    result = runner.invoke(app, ["schedule", "example", "0 12 * * *"])

    assert result.exit_code == 0
    data = yaml.safe_load((tmp_path / "sched.yml").read_text())
    assert data["ExampleTask"] == "0 12 * * *"


def test_cli_schedule_unknown_task(monkeypatch):
    from task_cascadence.scheduler import CronScheduler

    sched = CronScheduler(storage_path="/tmp/dummy.yml")
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)

    runner = CliRunner()
    result = runner.invoke(app, ["schedule", "missing", "* * * * *"])

    assert result.exit_code == 1


def test_global_transport_grpc(monkeypatch):
    captured = {}

    def fake_config(transport, **kwargs):
        captured["transport"] = transport
        captured.update(kwargs)

    monkeypatch.setattr("task_cascadence.cli.ume.configure_transport", fake_config)

    mod = types.ModuleType("stubmod")
    mod.stub = object()
    sys.modules["stubmod"] = mod

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--transport", "grpc", "--grpc-stub", "stubmod:stub", "list"],
    )

    assert result.exit_code == 0
    assert captured == {"transport": "grpc", "stub": mod.stub, "method": "Send"}


def test_global_transport_nats(monkeypatch):
    captured = {}

    def fake_config(transport, **kwargs):
        captured["transport"] = transport
        captured.update(kwargs)

    monkeypatch.setattr("task_cascadence.cli.ume.configure_transport", fake_config)

    mod = types.ModuleType("connmod")
    mod.conn = object()
    sys.modules["connmod"] = mod

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["--transport", "nats", "--nats-conn", "connmod:conn", "list"],
    )

    assert result.exit_code == 0
    assert captured == {"transport": "nats", "connection": mod.conn, "subject": "events"}


