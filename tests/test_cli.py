from click.exceptions import UsageError
from typing import Any

import pytest
import time
import asyncio
import threading
from typer.testing import CliRunner
from fastapi.testclient import TestClient

from task_cascadence.cli import app, main
from task_cascadence.api import app as api_app
from task_cascadence.plugins import ManualTrigger, CronTask
from task_cascadence.scheduler import get_default_scheduler, BaseScheduler, CronScheduler
from task_cascadence import initialize
from task_cascadence.temporal import TemporalBackend
from task_cascadence import ume
from task_cascadence.pipeline_registry import get_pipeline


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
    from typing import cast
    from task_cascadence.scheduler import CronScheduler

    sched = cast(CronScheduler, get_default_scheduler())
    sched.register_task(name_or_task="manual_demo", task_or_expr=ManualTask())

    from task_cascadence import ume

    monkeypatch.setattr(
        ume,
        "emit_task_run",
        lambda run, user_id=None, group_id=None: None,
    )

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["trigger", "manual_demo", "--user-id", "bob", "--group-id", "team"],
    )

    assert result.exit_code == 0


class DummyTask(CronTask):
    pass


def test_run_command_temporal(monkeypatch):
    backend = TemporalBackend()
    initialize()
    sched = get_default_scheduler()
    sched._temporal = backend
    sched.register_task(name_or_task="dummy", task_or_expr=DummyTask())

    called = {}

    def fake_run(workflow):
        called["workflow"] = workflow
        return "remote"

    monkeypatch.setattr(backend, "run_workflow_sync", fake_run)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "run",
            "dummy",
            "--temporal",
            "--user-id",
            "bob",
            "--group-id",
            "team",
        ],
    )

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


def test_main_webhook_command(monkeypatch):
    """Running ``task webhook`` should start the server via Uvicorn."""

    called = {}

    def fake_run(app, host="0.0.0.0", port=8000):
        called["host"] = host
        called["port"] = port

    monkeypatch.setattr("task_cascadence.webhook.uvicorn.run", fake_run)

    main(["webhook", "--host", "127.0.0.1", "--port", "9000"])

    assert called == {"host": "127.0.0.1", "port": 9000}


def test_cli_schedule_creates_entry(monkeypatch, tmp_path):
    from task_cascadence.scheduler import CronScheduler
    from task_cascadence.plugins import ExampleTask
    import yaml

    sched = CronScheduler(storage_path=tmp_path / "sched.yml")
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)
    sched.register_task(name_or_task="example", task_or_expr=ExampleTask())

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "schedule",
            "example",
            "0 12 * * *",
            "--user-id",
            "alice",
            "--group-id",
            "ops",
        ],
    )

    assert result.exit_code == 0
    from task_cascadence.ume import _hash_user_id

    data = yaml.safe_load((tmp_path / "sched.yml").read_text())
    assert data["ExampleTask"]["expr"] == "0 12 * * *"
    assert data["ExampleTask"]["user_hash"] == _hash_user_id("alice")


def test_cli_schedule_user_id(monkeypatch, tmp_path):
    from task_cascadence.scheduler import CronScheduler
    from task_cascadence.plugins import ExampleTask
    import yaml

    sched = CronScheduler(storage_path=tmp_path / "sched.yml")
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)
    sched.register_task(name_or_task="example", task_or_expr=ExampleTask())

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "schedule",
            "example",
            "0 12 * * *",
            "--user-id",
            "charlie",
            "--group-id",
            "ops",
        ],
    )

    assert result.exit_code == 0
    from task_cascadence.ume import _hash_user_id

    data = yaml.safe_load((tmp_path / "sched.yml").read_text())
    assert data["ExampleTask"]["expr"] == "0 12 * * *"
    assert data["ExampleTask"]["user_hash"] == _hash_user_id("charlie")


def test_cli_schedule_unknown_task(monkeypatch):
    from task_cascadence.scheduler import CronScheduler

    sched = CronScheduler(storage_path="/tmp/dummy.yml")
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "schedule",
            "missing",
            "* * * * *",
            "--user-id",
            "alice",
            "--group-id",
            "ops",
        ],
    )

    assert result.exit_code == 1


def test_cli_schedule_requires_group_id(monkeypatch):
    from task_cascadence.scheduler import CronScheduler
    from task_cascadence.plugins import ExampleTask

    sched = CronScheduler(storage_path="/tmp/dummy.yml")
    sched.register_task(name_or_task="example", task_or_expr=ExampleTask())
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["schedule", "example", "* * * * *", "--user-id", "alice"],
    )

    assert result.exit_code == 2
    assert "--group-id" in result.stderr


def test_cli_schedule_requires_cron_scheduler(monkeypatch):
    from task_cascadence.scheduler import BaseScheduler
    from task_cascadence.plugins import ExampleTask

    sched = BaseScheduler()
    sched.register_task("example", ExampleTask())
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "schedule",
            "example",
            "0 12 * * *",
            "--user-id",
            "alice",
            "--group-id",
            "ops",
        ],
    )

    assert result.exit_code == 1
    assert "scheduler lacks cron capabilities" in result.output

def test_cli_schedule_env_base(monkeypatch):
    """task schedule should fail without a cron scheduler"""
    monkeypatch.setenv("CASCADENCE_SCHEDULER", "base")
    import importlib
    import task_cascadence
    importlib.reload(task_cascadence)
    task_cascadence.initialize()
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "schedule",
            "example",
            "0 12 * * *",
            "--user-id",
            "alice",
            "--group-id",
            "ops",
        ],
    )
    assert result.exit_code == 1
    assert "scheduler lacks cron capabilities" in result.output

def test_cli_replay_history(monkeypatch):
    backend = TemporalBackend()
    scheduler = BaseScheduler(temporal=backend)

    monkeypatch.setattr("task_cascadence.cli.default_scheduler", scheduler)

    called = {}

    def fake_replay(path):
        called["path"] = path

    monkeypatch.setattr(backend, "replay", fake_replay)

    runner = CliRunner()
    result = runner.invoke(app, ["replay-history", "history.json"])

    assert result.exit_code == 0
    assert called["path"] == "history.json"


class DummyStub:
    def Send(self, msg, timeout=None):
        pass


grpc_stub_for_tests = DummyStub()


class DummyConn:
    """Simple stand-in for a NATS connection."""


dummy_nats_conn = DummyConn()


def test_cli_transport_option(monkeypatch):
    initialize()

    called = {}

    def fake_configure_transport(name, **kwargs):
        called["name"] = name
        called.update(kwargs)

    monkeypatch.setattr(ume, "configure_transport", fake_configure_transport)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "--transport",
            "grpc",
            "--grpc-stub",
            "tests.test_cli:grpc_stub_for_tests",
            "list",
        ],
    )

    assert result.exit_code == 0
    assert called == {
        "name": "grpc",
        "stub": grpc_stub_for_tests,
        "method": "Send",
    }


def test_cli_transport_missing_grpc_stub(monkeypatch):
    """Missing --grpc-stub should cause an error for gRPC transport."""

    initialize()

    runner = CliRunner()
    result = runner.invoke(app, ["--transport", "grpc", "list"])

    assert result.exit_code == 2
    assert "--grpc-stub is required for grpc transport" in result.stderr


def test_cli_transport_option_nats(monkeypatch):
    """The CLI should support configuring the NATS transport."""

    initialize()

    called = {}

    def fake_configure_transport(name, **kwargs):
        called["name"] = name
        called.update(kwargs)

    monkeypatch.setattr(ume, "configure_transport", fake_configure_transport)

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "--transport",
            "nats",
            "--nats-conn",
            "tests.test_cli:dummy_nats_conn",
            "--nats-subject",
            "demo",
            "list",
        ],
    )

    assert result.exit_code == 0
    assert called == {
        "name": "nats",
        "connection": dummy_nats_conn,
        "subject": "demo",
    }


def test_cli_transport_missing_nats_conn(monkeypatch):
    """Missing --nats-conn should raise an error for NATS transport."""

    initialize()

    runner = CliRunner()
    result = runner.invoke(app, ["--transport", "nats", "list"])

    assert result.exit_code == 2
    assert "--nats-conn is required for nats transport" in result.stderr


def test_cli_transport_unknown(monkeypatch):
    """Unknown transport value should be rejected."""

    initialize()

    runner = CliRunner()
    result = runner.invoke(app, ["--transport", "foo", "list"])

    assert result.exit_code == 2
    assert "Unknown transport: foo" in result.stderr


def test_cli_run_user_id(monkeypatch):
    initialize()
    from typing import cast
    from task_cascadence.scheduler import CronScheduler

    sched = cast(CronScheduler, get_default_scheduler())
    sched.register_task(name_or_task="manual_demo", task_or_expr=ManualTask())

    captured = {}

    from task_cascadence.ume import _hash_user_id

    def fake_emit(run, user_id=None, group_id=None):
        if user_id is not None:
            run.user_hash = _hash_user_id(user_id)
        captured["run"] = run

    monkeypatch.setattr(ume, "emit_task_run", fake_emit)

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", "manual_demo", "--user-id", "alice", "--group-id", "ops"],
    )

    assert result.exit_code == 0
    assert "run" in captured
    assert captured["run"].user_hash is not None
    assert captured["run"].user_hash != "alice"


def test_cli_run_requires_group_id(monkeypatch):
    initialize()
    from typing import cast
    from task_cascadence.scheduler import CronScheduler

    sched = cast(CronScheduler, get_default_scheduler())
    sched.register_task(name_or_task="manual_demo", task_or_expr=ManualTask())

    runner = CliRunner()
    result = runner.invoke(app, ["run", "manual_demo", "--user-id", "alice"])

    assert result.exit_code == 2
    assert "--group-id" in result.stderr


def test_cli_schedules_lists_entries(monkeypatch, tmp_path):
    from task_cascadence.scheduler import CronScheduler
    from task_cascadence.plugins import ExampleTask

    sched = CronScheduler(storage_path=tmp_path / "sched.yml")
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)

    sched.register_task(name_or_task=ExampleTask(), task_or_expr="0 5 * * *")

    runner = CliRunner()
    result = runner.invoke(app, ["schedules"])

    assert result.exit_code == 0
    assert result.output == "ExampleTask\t0 5 * * *\n"


def test_cli_schedules_empty(monkeypatch, tmp_path):
    from task_cascadence.scheduler import CronScheduler

    sched = CronScheduler(storage_path=tmp_path / "sched.yml")
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)

    runner = CliRunner()
    result = runner.invoke(app, ["schedules"])

    assert result.exit_code == 0
    assert result.output == ""


def test_cli_pointer_add_and_list(monkeypatch, tmp_path):
    from task_cascadence.scheduler import BaseScheduler
    from task_cascadence.plugins import PointerTask
    import yaml

    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    store = tmp_path / "pointers.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(store))

    class DemoPointer(PointerTask):
        name = "demo_pointer"

    sched = BaseScheduler()
    task = DemoPointer()
    sched.register_task("demo_pointer", task)
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)

    runner = CliRunner()
    result = runner.invoke(app, ["pointer-add", "demo_pointer", "alice", "run1"])
    assert result.exit_code == 0
    assert task.get_pointers()[0].run_id == "run1"

    result = runner.invoke(app, ["pointer-list", "demo_pointer"])
    assert result.exit_code == 0
    data = yaml.safe_load(store.read_text())
    assert data["demo_pointer"][0]["run_id"] == "run1"


def test_cli_run_pipeline_task(monkeypatch):
    steps = []

    class PipelineDemo:
        name = "pipe_demo"

        def __init__(self, steps):
            self.steps = steps

        def intake(self):
            self.steps.append("intake")

        def run(self):
            self.steps.append("run")
            return "ok"

    sched = BaseScheduler()
    sched.register_task("pipe_demo", PipelineDemo(steps))

    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)
    monkeypatch.setattr("task_cascadence.ume.emit_stage_update", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda *a, **k: None)

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", "pipe_demo", "--user-id", "alice", "--group-id", "ops"],
    )

    assert result.exit_code == 0
    assert steps == ["intake", "run"]


def test_cli_run_outputs_run_id_and_supports_signal(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "cli_stages.yml"))
    import task_cascadence.ume as ume_mod

    ume_mod._stage_store = None

    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_stage_update_event", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_audit_log", lambda *a, **k: None)

    class CliContextTask(CronTask):
        name = "cli-context"

        def __init__(self) -> None:
            self.research_started = threading.Event()
            self.resume = threading.Event()
            self.plan_contexts: list[list[dict[str, str]]] = []

        def intake(self) -> None:
            pass

        def research(self) -> None:
            self.research_started.set()
            if not self.resume.wait(timeout=1):
                raise AssertionError("research did not resume")

        def plan(self) -> dict[str, str]:
            snapshot = list(self.context)
            self.plan_contexts.append(snapshot)
            return snapshot[-1] if snapshot else {}

        def run(self, plan_result: dict[str, str]) -> dict[str, str]:
            return plan_result

    sched = CronScheduler(storage_path=tmp_path / "sched.yml")
    task = CliContextTask()
    sched.register_task(name_or_task="cli-context", task_or_expr=task)

    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)
    monkeypatch.setattr("task_cascadence.api.get_default_scheduler", lambda: sched)

    client = TestClient(api_app)
    runner = CliRunner()
    result_box: dict[str, Any] = {}

    def invoke_cli() -> None:
        result_box["result"] = runner.invoke(
            app,
            ["run", "cli-context", "--user-id", "alice", "--group-id", "team"],
        )

    thread = threading.Thread(target=invoke_cli, daemon=True)
    thread.start()

    assert task.research_started.wait(timeout=1)

    pipeline = get_pipeline("cli-context")
    assert pipeline is not None
    run_id = pipeline.current_run_id
    assert run_id is not None

    resp = client.post(
        f"/tasks/{run_id}/signal",
        headers={"X-User-ID": "alice", "X-Group-ID": "team"},
        json={"kind": "context", "value": {"note": "cli"}},
    )
    assert resp.status_code == 202

    task.resume.set()
    thread.join(timeout=2)
    assert not thread.is_alive()

    result = result_box["result"]
    assert result.exit_code == 0
    lines = [line for line in result.output.splitlines() if line.startswith("run-id")]
    assert lines
    reported_run_id = lines[-1].split("\t", 1)[1]
    assert reported_run_id == run_id
    assert task.plan_contexts == [[{"note": "cli"}]]

def test_cli_watch_plugins(monkeypatch, tmp_path):
    events = []

    class DummyWatcher:
        def __init__(self, path):
            events.append(("init", path))

        def start(self):
            events.append("start")

        def stop(self):
            events.append("stop")

    monkeypatch.setattr(
        "task_cascadence.plugins.watcher.PluginWatcher",
        DummyWatcher,
    )

    monkeypatch.setattr(time, "sleep", lambda _: (_ for _ in ()).throw(KeyboardInterrupt()))

    runner = CliRunner()
    result = runner.invoke(app, ["watch-plugins", str(tmp_path)])

    assert result.exit_code == 0
    assert events == [("init", str(tmp_path)), "start", "stop"]


def test_cli_unschedule(monkeypatch, tmp_path):
    from task_cascadence.scheduler import CronScheduler
    from task_cascadence.plugins import ExampleTask
    import yaml

    sched = CronScheduler(storage_path=tmp_path / "sched.yml")
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)
    sched.register_task(name_or_task="example", task_or_expr=ExampleTask())

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "schedule",
            "example",
            "0 12 * * *",
            "--user-id",
            "alice",
            "--group-id",
            "ops",
        ],
    )
    assert result.exit_code == 0

    result = runner.invoke(app, ["unschedule", "example"])
    assert result.exit_code == 0

    assert sched.scheduler.get_job("ExampleTask") is None
    data = yaml.safe_load((tmp_path / "sched.yml").read_text()) or {}
    assert "ExampleTask" not in data


def test_cli_unschedule_unknown(monkeypatch):
    from task_cascadence.scheduler import CronScheduler

    sched = CronScheduler(storage_path="/tmp/sched.yml")
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)

    runner = CliRunner()
    result = runner.invoke(app, ["unschedule", "missing"])
    assert result.exit_code == 1


def test_cli_run_async(monkeypatch):
    steps = []

    class AsyncDemo:
        name = "async_demo"

        async def run(self):
            await asyncio.sleep(0)
            steps.append("run")
            return "ok"

    sched = BaseScheduler()
    sched.register_task("async_demo", AsyncDemo())

    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)
    monkeypatch.setattr("task_cascadence.ume.emit_stage_update", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda *a, **k: None)

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run-async", "async_demo", "--user-id", "alice", "--group-id", "ops"],
    )


    assert result.exit_code == 0
    assert steps == ["run"]


def test_cli_run_async_user_id(monkeypatch):
    steps = []

    class AsyncDemo:
        name = "async_demo"

        async def run(self):
            await asyncio.sleep(0)
            steps.append("run")
            return "ok"

    sched = BaseScheduler()
    sched.register_task("async_demo", AsyncDemo())

    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)
    monkeypatch.setattr("task_cascadence.ume.emit_stage_update", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)

    captured = {}
    from task_cascadence import ume
    from task_cascadence.ume import _hash_user_id

    def fake_emit(run, user_id=None, group_id=None):
        if user_id is not None:
            run.user_hash = _hash_user_id(user_id)
        captured["run"] = run

    monkeypatch.setattr(ume, "emit_task_run", fake_emit)

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run-async", "async_demo", "--user-id", "bob", "--group-id", "ops"],
    )

    assert result.exit_code == 0
    assert steps == ["run"]
    assert "run" in captured
    assert captured["run"].user_hash is not None
    assert captured["run"].user_hash != "bob"


def test_cli_disable_prevents_run(monkeypatch):
    class Demo:
        name = "demo"

        def run(self):
            return "ok"

    sched = BaseScheduler()
    sched.register_task("demo", Demo())
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)

    runner = CliRunner()
    result = runner.invoke(app, ["disable", "demo", "--group-id", "ops"])
    assert result.exit_code == 0
    assert "demo disabled" in result.output

    result = runner.invoke(
        app,
        ["run", "demo", "--user-id", "alice", "--group-id", "ops"],
    )
    assert result.exit_code != 0
    assert "disabled" in (result.stderr or result.output)



