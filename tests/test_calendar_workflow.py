
from typing import Any

import importlib.util
from pathlib import Path
import sys
import types

import pytest

PACKAGE_ROOT = Path(__file__).resolve().parents[1] / "task_cascadence"
pkg = types.ModuleType("task_cascadence")
pkg.__path__ = [str(PACKAGE_ROOT)]
sys.modules["task_cascadence"] = pkg


def _load(name: str, rel: str):
    spec = importlib.util.spec_from_file_location(
        name, PACKAGE_ROOT / rel
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    sys.modules[name] = module
    return module


_load("task_cascadence.http_utils", "http_utils.py")
_load("task_cascadence.research", "research.py")
_load("task_cascadence.ume", "ume/__init__.py")
workflows = _load("task_cascadence.workflows", "workflows/__init__.py")
cec = _load(
    "task_cascadence.workflows.calendar_event_creation",
    "workflows/calendar_event_creation.py",
)
dispatch = workflows.dispatch


class DummyResponse:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


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

    def fake_emit(name, stage, user_id=None, group_id=None, **kwargs):
        emitted["event"] = (name, stage, user_id, group_id, kwargs.get("event_id"))

    async def fake_async_gather(query, user_id=None, group_id=None):
        emitted["async_research"] = (query, user_id, group_id)
        return {"duration": "15m"}

    def fake_gather(query, user_id=None, group_id=None):
        emitted["gather"] = (query, user_id, group_id)
        return {"duration": "15m"}

    def fake_emit_note(note, user_id=None, group_id=None):
        emitted["note"] = (note.note, user_id, group_id)


    monkeypatch.setattr(cec, "request_with_retry", fake_request)
    monkeypatch.setattr(cec, "emit_stage_update_event", fake_emit)
    monkeypatch.setattr(cec.research, "async_gather", fake_async_gather)
    monkeypatch.setattr(cec.research, "gather", fake_gather)
    monkeypatch.setattr(cec.research, "async_gather", fake_async_gather)
    monkeypatch.setattr(cec, "emit_task_note", fake_emit_note)

    def fake_dispatch(event, data, *, user_id, group_id=None):
        dispatched["event"] = (event, data, user_id, group_id)

    monkeypatch.setattr(cec, "dispatch", fake_dispatch)


    payload = {
        "title": "Lunch",
        "start_time": "2024-01-01T12:00:00Z",
        "location": "Cafe",
        "group_id": "g1",
        "invitees": ["bob"],
        "layers": ["work"],
    }

    result = dispatch(
        "calendar.event.create_request", payload, user_id="alice", base_url="http://svc"
    )

    assert result == {"event_id": "evt1", "related_event_id": "evt2"}

    # permission checks
    assert calls[0][0] == "GET"
    assert "permissions" in calls[0][1]

    # event creation and related event persistence
    assert calls[3][0] == "POST"
    assert calls[3][1] == "http://svc/v1/calendar/events"
    assert calls[3][2]["json"]["travel_time"] == {"duration": "15m"}

    # invite-edge persistence
    assert calls[5][1] == "http://svc/v1/calendar/edges"
    edge_payload = calls[5][2]["json"]
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
    assert emitted["gather"] == ("travel time to Cafe", "alice", "g1")
    assert emitted["note"] == ("Travel time to Cafe: 15m", "alice", "g1")


@pytest.mark.parametrize("missing_field", ["title", "start_time"])
def test_calendar_event_creation_missing_field(monkeypatch, missing_field):
    calls: list[str] = []

    def fake_request(method, url, timeout, **kwargs):
        calls.append(method)
        return DummyResponse({})

    monkeypatch.setattr(cec, "request_with_retry", fake_request)
    emitted: dict[str, tuple[Any, ...]] = {}
    monkeypatch.setattr(
        cec, "emit_stage_update_event", lambda *a, **k: emitted.setdefault("event", (a, k))
    )

    payload = {"title": "Lunch", "start_time": "2024-01-01T12:00:00Z"}
    payload.pop(missing_field)

    with pytest.raises(ValueError):
        dispatch("calendar.event.create_request", payload, user_id="alice")

    assert "POST" not in calls
    assert "event" not in emitted


def test_calendar_event_creation_permission_denied(monkeypatch):
    calls: list[str] = []

    def fake_request(method, url, timeout, **kwargs):
        calls.append(method)
        return DummyResponse({})

    monkeypatch.setattr(cec, "request_with_retry", fake_request)
    monkeypatch.setattr(cec, "_has_permission", lambda *a, **k: False)

    emitted: dict[str, tuple[Any, ...]] = {}
    monkeypatch.setattr(
        cec, "emit_stage_update_event", lambda *a, **k: emitted.setdefault("event", (a, k))
    )

    payload = {"title": "Lunch", "start_time": "2024-01-01T12:00:00Z"}

    with pytest.raises(PermissionError):
        dispatch("calendar.event.create_request", payload, user_id="alice")

    assert "POST" not in calls
    assert "event" not in emitted


def test_calendar_event_creation_research_failure(monkeypatch):
    calls: list[str] = []

    def fake_request(method, url, timeout, **kwargs):
        calls.append(method)
        return DummyResponse({})

    monkeypatch.setattr(cec, "request_with_retry", fake_request)
    monkeypatch.setattr(cec, "_has_permission", lambda *a, **k: True)

    async def fail_async(*a, **k):
        raise KeyboardInterrupt("gather boom")

    monkeypatch.setattr(cec.research, "async_gather", fail_async)

    emitted: dict[str, tuple[Any, ...]] = {}
    monkeypatch.setattr(
        cec, "emit_stage_update_event", lambda *a, **k: emitted.setdefault("event", (a, k))
    )
    monkeypatch.setattr(cec, "emit_task_note", lambda *a, **k: None)

    payload = {
        "title": "Lunch",
        "start_time": "2024-01-01T12:00:00Z",
        "location": "Cafe",
    }

    with pytest.raises(KeyboardInterrupt):
        dispatch("calendar.event.create_request", payload, user_id="alice", base_url="http://svc")

    assert "POST" not in calls
    assert "event" not in emitted


