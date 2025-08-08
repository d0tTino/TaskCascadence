from task_cascadence.scheduler import BaseScheduler
from task_cascadence.plugins import CronTask
from task_cascadence.temporal import TemporalBackend
import task_cascadence.temporal as temporal
import inspect
from unittest.mock import AsyncMock, Mock
import asyncio


class DummyTask(CronTask):
    pass


def test_run_task_via_temporal(monkeypatch):
    backend = TemporalBackend()
    scheduler = BaseScheduler(temporal=backend)
    task = DummyTask()
    scheduler.register_task("dummy", task)

    called = {}

    def fake_run(workflow):
        called["workflow"] = workflow
        return "remote"

    monkeypatch.setattr(backend, "run_workflow_sync", fake_run)

    result = scheduler.run_task("dummy", user_id="alice")
    assert result == "remote"
    assert called["workflow"] == "DummyTask"


def test_replay_history(monkeypatch):
    backend = TemporalBackend()
    scheduler = BaseScheduler(temporal=backend)

    called = {}

    def fake_replay(path):
        called["path"] = path

    monkeypatch.setattr(backend, "replay", fake_replay)
    scheduler.replay_history("file.json")
    assert called["path"] == "file.json"


def test_run_workflow_sync_executes_via_asyncio(monkeypatch):
    backend = TemporalBackend()

    client = Mock()
    client.execute_workflow = AsyncMock(return_value="ok")

    connect_mock = AsyncMock(return_value=client)
    monkeypatch.setattr(temporal.Client, "connect", connect_mock)

    run_called = {}
    orig_run = asyncio.run

    def fake_run(coro, *args, **kwargs):
        run_called["called"] = True
        return orig_run(coro, *args, **kwargs)

    monkeypatch.setattr(asyncio, "run", fake_run)

    result = backend.run_workflow_sync("DemoWorkflow", 1, foo="bar")

    assert result == "ok"
    assert run_called.get("called") is True
    connect_mock.assert_awaited_once()
    args, kwargs = connect_mock.call_args
    assert args == ("localhost:7233",)
    if "runtime" in inspect.signature(temporal.Client.connect).parameters:
        assert "runtime" in kwargs
    else:
        assert kwargs == {}
    client.execute_workflow.assert_awaited_once_with("DemoWorkflow", 1, foo="bar")


def test_replay_uses_replayer(monkeypatch):
    backend = TemporalBackend()

    replayer = Mock()
    monkeypatch.setattr(temporal, "Replayer", lambda: replayer)

    backend.replay("history.json")

    replayer.replay.assert_called_once_with("history.json")


def test_backend_server_from_env(monkeypatch):
    monkeypatch.setenv("TEMPORAL_SERVER", "remote:4444")
    backend = TemporalBackend()
    assert backend.server == "remote:4444"
