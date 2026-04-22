from task_cascadence.scheduler import BaseScheduler
from task_cascadence.plugins import CronTask
from task_cascadence.temporal import TemporalBackend
import task_cascadence.temporal as temporal
import inspect
from unittest.mock import AsyncMock, Mock
import asyncio
import pytest


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

    execution = scheduler.run_task_with_metadata("dummy", user_id="alice")
    assert execution.result == "remote"
    assert execution.run_id
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


def test_signal_workflow_uses_handle(monkeypatch):
    backend = TemporalBackend()
    handle = Mock()
    handle.signal = AsyncMock(return_value="ack")

    client = Mock()
    client.get_workflow_handle = Mock(return_value=handle)
    connect_mock = AsyncMock(return_value=client)
    monkeypatch.setattr(backend, "connect", connect_mock)

    result = asyncio.run(
        backend.signal_workflow(
            "workflow-123",
            "attach_context",
            {"user_id": "alice"},
            run_id="run-456",
            metadata={"source": "api"},
        )
    )

    assert result == "ack"
    client.get_workflow_handle.assert_called_once_with("workflow-123", run_id="run-456")
    handle.signal.assert_awaited_once_with(
        "attach_context",
        {"user_id": "alice"},
        metadata={"source": "api"},
    )


def test_query_workflow_uses_handle(monkeypatch):
    backend = TemporalBackend()
    handle = Mock()
    handle.query = AsyncMock(return_value={"status": "ready"})

    client = Mock()
    client.get_workflow_handle = Mock(return_value=handle)
    connect_mock = AsyncMock(return_value=client)
    monkeypatch.setattr(backend, "connect", connect_mock)

    result = asyncio.run(
        backend.query_workflow(
            "workflow-abc",
            "fetch_context",
            "scope-a",
            run_id="run-def",
            include_history=True,
        )
    )

    assert result == {"status": "ready"}
    client.get_workflow_handle.assert_called_once_with("workflow-abc", run_id="run-def")
    handle.query.assert_awaited_once_with("fetch_context", "scope-a", include_history=True)


def test_signal_workflow_sync_uses_run_coroutine(monkeypatch):
    backend = TemporalBackend()
    monkeypatch.setattr(backend, "signal_workflow", AsyncMock(return_value="synced"))

    result = backend.signal_workflow_sync(
        "workflow-1",
        "attach_context",
        {"group_id": "eng"},
        run_id="run-1",
    )

    assert result == "synced"
    backend.signal_workflow.assert_awaited_once_with(  # type: ignore[attr-defined]
        "workflow-1",
        "attach_context",
        {"group_id": "eng"},
        run_id="run-1",
    )


def test_query_workflow_sync_uses_run_coroutine(monkeypatch):
    backend = TemporalBackend()
    monkeypatch.setattr(backend, "query_workflow", AsyncMock(return_value={"state": "ok"}))

    result = backend.query_workflow_sync(
        "workflow-2",
        "current_context",
        run_id="run-2",
        with_details=True,
    )

    assert result == {"state": "ok"}
    backend.query_workflow.assert_awaited_once_with(  # type: ignore[attr-defined]
        "workflow-2",
        "current_context",
        run_id="run-2",
        with_details=True,
    )


def test_signal_workflow_propagates_error(monkeypatch):
    backend = TemporalBackend()
    handle = Mock()
    handle.signal = AsyncMock(side_effect=RuntimeError("signal failed"))
    client = Mock()
    client.get_workflow_handle = Mock(return_value=handle)
    monkeypatch.setattr(backend, "connect", AsyncMock(return_value=client))

    with pytest.raises(RuntimeError, match="signal failed"):
        asyncio.run(backend.signal_workflow("workflow-err", "attach_context"))


def test_query_workflow_propagates_error(monkeypatch):
    backend = TemporalBackend()
    handle = Mock()
    handle.query = AsyncMock(side_effect=RuntimeError("query failed"))
    client = Mock()
    client.get_workflow_handle = Mock(return_value=handle)
    monkeypatch.setattr(backend, "connect", AsyncMock(return_value=client))

    with pytest.raises(RuntimeError, match="query failed"):
        asyncio.run(backend.query_workflow("workflow-err", "current_context"))
