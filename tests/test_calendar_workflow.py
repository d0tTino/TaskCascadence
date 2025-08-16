
# Tests for calendar event creation workflow

import pytest

import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from task_cascadence.workflows import dispatch
from task_cascadence.workflows import (
    calendar_event_creation as cec,
)



class DummyResponse:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


def test_calendar_event_creation_module_imports():
    import task_cascadence.workflows.calendar_event_creation as mod

    assert mod is not None


def test_calendar_event_creation_importable():
    import importlib
    import sys

    sys.modules.pop("task_cascadence.workflows.calendar_event_creation", None)
    module = importlib.import_module(
        "task_cascadence.workflows.calendar_event_creation"
    )
    globals()["cec"] = module


def test_calendar_event_creation(monkeypatch):
    calls = []
    counter = {"post": 0}

    def fake_request(method, url, timeout, **kwargs):
        calls.append((method, url, kwargs))
        if method == "GET":
            return DummyResponse({"allowed": True})
        if url.endswith("/edges"):
            return DummyResponse({"ok": True})
        counter["post"] += 1
        return DummyResponse({"id": f"evt{counter['post']}"})

    emitted: dict[str, tuple[Any, ...]] = {}
    dispatched: dict[str, tuple[Any, ...]] = {}
    audit_logs: list[tuple[str, str, str, str | None, str | None]] = []

    def fake_emit(name, stage, user_id=None, group_id=None, **kwargs):
        emitted["event"] = (name, stage, user_id, group_id, kwargs.get("event_id"))

    async def fake_async_gather(query, user_id=None, group_id=None):
        emitted["async_research"] = (query, user_id, group_id)
        return {"duration": "15m"}

    def fake_emit_note(note, user_id=None, group_id=None):
        emitted["note"] = (note.note, user_id, group_id)


    monkeypatch.setattr(cec, "request_with_retry", fake_request)
    monkeypatch.setattr(cec, "emit_stage_update_event", fake_emit)
    monkeypatch.setattr(cec.research, "async_gather", fake_async_gather)
    monkeypatch.setattr(cec, "emit_task_note", fake_emit_note)
    monkeypatch.setattr(
        cec,
        "emit_audit_log",
        lambda task, stage, status, *, reason=None, user_id=None, group_id=None, **_: audit_logs.append(
            (task, stage, status, user_id, group_id)
        ),
    )

    def fake_dispatch(event, data, *, user_id, group_id=None):
        dispatched["event"] = (event, data, user_id, group_id)

    monkeypatch.setattr(cec, "dispatch", fake_dispatch)


    payload = {
        "title": "Lunch",
        "start_time": "2024-01-01T12:00:00Z",
        "location": "Cafe",
        "invitees": ["bob"],
        "layers": ["work"],
    }

    result = dispatch(
        "calendar.event.create_request", payload, user_id="alice", group_id="g1", base_url="http://svc"
    )

    assert result == {"event_id": "evt1", "related_event_id": "evt2"}

    # permission checks
    assert calls[0][0] == "GET"
    assert "permissions" in calls[0][1]

    # event creation and related event persistence
    assert calls[2][0] == "POST"
    assert calls[2][1] == "http://svc/v1/calendar/events"
    assert calls[2][2]["json"]["travel_time"] == {"duration": "15m"}

    # invite-edge persistence
    assert calls[4][1] == "http://svc/v1/calendar/edges"
    edge_payload = calls[4][2]["json"]
    assert edge_payload["src"] == "evt2"
    assert edge_payload["dst"] == "evt1"

    # event emission
    assert emitted["event"] == (
        "calendar.event.created",
        "created",
        "alice",
        "g1",
        "evt1",
    )
    assert emitted["async_research"] == ("travel time to Cafe", "alice", "g1")
    assert emitted["note"] == ("Travel time to Cafe: 15m", "alice", "g1")
    assert (
        "calendar.event.create",
        "workflow",
        "started",
        "alice",
        "g1",
    ) in audit_logs
    assert (
        "calendar.event.create",
        "workflow",
        "completed",
        "alice",
        "g1",
    ) in audit_logs


def test_calendar_event_permission_error(monkeypatch):
    def fake_request(method, url, timeout, **kwargs):
        if method == "GET":
            raise RuntimeError("network down")
        return DummyResponse({"id": "evt1"})

    audit_logs: list[tuple[str, str, str]] = []

    def fake_emit_audit_log(task_name, stage, status, *, reason=None, user_id=None, group_id=None, **_):
        audit_logs.append((task_name, stage, status, reason))

    monkeypatch.setattr(cec, "request_with_retry", fake_request)
    monkeypatch.setattr(cec, "emit_audit_log", fake_emit_audit_log)

    payload = {"title": "Lunch", "start_time": "2024-01-01T12:00:00Z"}

    with pytest.raises(RuntimeError):
        dispatch("calendar.event.create_request", payload, user_id="alice")

    assert (
        "calendar.event.create",
        "workflow",
        "started",
        None,
    ) in audit_logs
    assert (
        "calendar.event.create",
        "permission",
        "error",
        "network down",
    ) in audit_logs


def test_calendar_event_group_id_mismatch(monkeypatch):
    audit_logs: list[tuple[str, str, str, str | None, str | None, str | None]] = []

    def fake_emit_audit_log(
        task, stage, status, *, reason=None, user_id=None, group_id=None, **_
    ):
        audit_logs.append((task, stage, status, reason, user_id, group_id))

    monkeypatch.setattr(cec, "emit_audit_log", fake_emit_audit_log)

    payload = {
        "title": "Lunch",
        "start_time": "2024-01-01T12:00:00Z",
        "group_id": "g2",
    }

    with pytest.raises(ValueError):
        dispatch("calendar.event.create_request", payload, user_id="alice", group_id="g1")

    assert (
        "calendar.event.create",
        "workflow",
        "error",
        "group_id mismatch",
        "alice",
        "g1",
    ) in audit_logs


def test_calendar_event_ume_failure(monkeypatch):
    calls = []
    counter = {"post": 0}

    def fake_request(method, url, timeout, **kwargs):
        calls.append((method, url, kwargs))
        if method == "GET":
            return DummyResponse({"allowed": True})
        if url.endswith("/edges"):
            return DummyResponse({"ok": True})
        counter["post"] += 1
        return DummyResponse({"id": f"evt{counter['post']}"})

    async def fake_async_gather(query, user_id=None, group_id=None):
        return {"duration": "15m"}

    audit_logs: list[tuple[str, str, str, str | None]] = []

    def fake_emit_audit_log(task_name, stage, status, *, reason=None, user_id=None, group_id=None, **_):
        audit_logs.append((task_name, stage, status, reason))

    def fake_emit(*a, **k):
        pass

    def failing_emit_note(note, user_id=None, group_id=None):
        raise RuntimeError("ume down")

    monkeypatch.setattr(cec, "request_with_retry", fake_request)
    monkeypatch.setattr(cec.research, "async_gather", fake_async_gather)
    monkeypatch.setattr(cec, "emit_stage_update_event", fake_emit)
    monkeypatch.setattr(cec, "emit_task_note", failing_emit_note)
    monkeypatch.setattr(cec, "emit_audit_log", fake_emit_audit_log)

    def fake_dispatch(event, data, *, user_id, group_id=None):
        pass

    monkeypatch.setattr(cec, "dispatch", fake_dispatch)

    payload = {
        "title": "Lunch",
        "start_time": "2024-01-01T12:00:00Z",
        "location": "Cafe",
    }

    result = dispatch(
        "calendar.event.create_request", payload, user_id="alice", base_url="http://svc"
    )

    assert result == {"event_id": "evt1", "related_event_id": "evt2"}
    assert (
        "calendar.event.create",
        "workflow",
        "started",
        None,
    ) in audit_logs
    assert (
        "calendar.event.create",
        "emit_task_note",
        "error",
        "ume down",
    ) in audit_logs
    assert (
        "calendar.event.create",
        "workflow",
        "completed",
        None,
    ) in audit_logs


def test_calendar_event_research_failure(monkeypatch):
    calls = []

    def fake_request(method, url, timeout, **kwargs):
        calls.append((method, url, kwargs))
        if method == "GET":
            return DummyResponse({"allowed": True})
        if url.endswith("/edges"):
            return DummyResponse({"ok": True})
        return DummyResponse({"id": "evt1"})

    def failing_async_gather(query, user_id=None, group_id=None):
        raise RuntimeError("research down")

    audit_logs: list[tuple[str, str, str, str | None]] = []

    def fake_emit_audit_log(task, stage, status, *, reason=None, user_id=None, group_id=None, **_):
        audit_logs.append((task, stage, status, reason))

    emitted_notes: list[str] = []

    def fake_emit_note(note, user_id=None, group_id=None):
        emitted_notes.append(note.note)

    monkeypatch.setattr(cec, "request_with_retry", fake_request)
    monkeypatch.setattr(cec.research, "async_gather", failing_async_gather)
    monkeypatch.setattr(cec, "emit_stage_update_event", lambda *a, **k: None)
    monkeypatch.setattr(cec, "emit_task_note", fake_emit_note)
    monkeypatch.setattr(cec, "emit_audit_log", fake_emit_audit_log)
    monkeypatch.setattr(cec, "dispatch", lambda *a, **k: None)

    payload = {
        "title": "Lunch",
        "start_time": "2024-01-01T12:00:00Z",
        "location": "Cafe",
    }

    result = dispatch(
        "calendar.event.create_request", payload, user_id="alice", base_url="http://svc"
    )

    assert result == {"event_id": "evt1"}
    assert "travel_time" not in calls[1][2]["json"]
    assert emitted_notes == ["No travel details"]
    assert (
        "calendar.event.create",
        "research",
        "error",
        "research down",
    ) in audit_logs
    assert (
        "calendar.event.create",
        "workflow",
        "started",
        None,
    ) in audit_logs
    assert (
        "calendar.event.create",
        "workflow",
        "completed",
        None,
    ) in audit_logs


@pytest.mark.asyncio
async def test_calendar_event_creation_in_event_loop(monkeypatch):
    counter = {"post": 0}

    def fake_request(method, url, timeout, **kwargs):
        if method == "GET":
            return DummyResponse({"allowed": True})
        if url.endswith("/edges"):
            return DummyResponse({"ok": True})
        counter["post"] += 1
        return DummyResponse({"id": f"evt{counter['post']}"})

    async def fake_async_gather(query, user_id=None, group_id=None):
        return {"duration": "15m"}

    audit_logs: list[tuple[str, str, str]] = []

    monkeypatch.setattr(cec, "request_with_retry", fake_request)
    monkeypatch.setattr(cec.research, "async_gather", fake_async_gather)
    monkeypatch.setattr(cec, "emit_stage_update_event", lambda *a, **k: None)
    monkeypatch.setattr(cec, "emit_task_note", lambda *a, **k: None)
    monkeypatch.setattr(
        cec,
        "emit_audit_log",
        lambda task, stage, status, *, reason=None, user_id=None, group_id=None, **_: audit_logs.append(
            (task, stage, status)
        ),
    )
    monkeypatch.setattr(cec, "dispatch", lambda *a, **k: None)

    payload = {
        "title": "Lunch",
        "start_time": "2024-01-01T12:00:00Z",
        "location": "Cafe",
    }

    result = dispatch("calendar.event.create_request", payload, user_id="alice")

    assert result == {"event_id": "evt1", "related_event_id": "evt2"}
    assert (
        "calendar.event.create",
        "workflow",
        "started",
    ) in audit_logs
    assert (
        "calendar.event.create",
        "workflow",
        "completed",
    ) in audit_logs


@pytest.mark.asyncio
async def test_calendar_event_research_failure_in_event_loop(monkeypatch):
    calls = []

    def fake_request(method, url, timeout, **kwargs):
        calls.append((method, url, kwargs))
        if method == "GET":
            return DummyResponse({"allowed": True})
        if url.endswith("/edges"):
            return DummyResponse({"ok": True})
        return DummyResponse({"id": "evt1"})

    async def failing_async_gather(query, user_id=None, group_id=None):
        raise RuntimeError("research down")

    audit_logs: list[tuple[str, str, str, str | None]] = []

    def fake_emit_audit_log(task, stage, status, *, reason=None, user_id=None, group_id=None, **_):
        audit_logs.append((task, stage, status, reason))

    emitted_notes: list[str] = []

    def fake_emit_note(note, user_id=None, group_id=None):
        emitted_notes.append(note.note)

    monkeypatch.setattr(cec, "request_with_retry", fake_request)
    monkeypatch.setattr(cec.research, "async_gather", failing_async_gather)
    monkeypatch.setattr(cec, "emit_stage_update_event", lambda *a, **k: None)
    monkeypatch.setattr(cec, "emit_task_note", fake_emit_note)
    monkeypatch.setattr(cec, "emit_audit_log", fake_emit_audit_log)
    monkeypatch.setattr(cec, "dispatch", lambda *a, **k: None)

    payload = {
        "title": "Lunch",
        "start_time": "2024-01-01T12:00:00Z",
        "location": "Cafe",
    }

    result = dispatch(
        "calendar.event.create_request", payload, user_id="alice", base_url="http://svc"
    )

    assert result == {"event_id": "evt1"}
    assert "travel_time" not in calls[1][2]["json"]
    assert emitted_notes == ["No travel details"]
    assert (
        "calendar.event.create",
        "research",
        "error",
        "research down",
    ) in audit_logs
    assert (
        "calendar.event.create",
        "workflow",
        "started",
        None,
    ) in audit_logs
    assert (
        "calendar.event.create",
        "workflow",
        "completed",
        None,
    ) in audit_logs


