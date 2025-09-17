import pytest

from task_cascadence.workflows import dispatch
from task_cascadence.workflows import calendar_event_creation as cec


class DummyResponse:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


def test_calendar_event_persistence_error(monkeypatch):
    def failing_request(method, url, timeout, **kwargs):
        if method == "GET":
            return DummyResponse({"allowed": True})
        raise RuntimeError("boom")

    audit_logs: list[tuple[str, str, str, str | None]] = []

    def fake_emit_audit_log(task, stage, status, *, reason=None, user_id=None, group_id=None, **_):
        audit_logs.append((task, stage, status, reason))

    monkeypatch.setattr(cec, "request_with_retry", failing_request)
    monkeypatch.setattr(cec, "emit_audit_log", fake_emit_audit_log)

    payload = {"title": "Lunch", "start_time": "2024-01-01T12:00:00Z"}

    with pytest.raises(RuntimeError) as excinfo:
        dispatch("calendar.event.create_request", payload, user_id="alice")

    error_reason = str(excinfo.value)

    assert (
        "calendar.event.create",
        "persistence",
        "error",
        error_reason,
    ) in audit_logs


def test_calendar_edge_persistence_error(monkeypatch):
    def edge_failing_request(method, url, timeout, **kwargs):
        if method == "GET":
            return DummyResponse({"allowed": True})
        if url.endswith("/v1/calendar/events"):
            return DummyResponse({"id": "evt1"})
        if url.endswith("/v1/calendar/edges"):
            raise RuntimeError("edge boom")
        raise AssertionError(f"unexpected request: {method} {url}")

    audit_logs: list[tuple[str, str, str, str | None]] = []

    def fake_emit_audit_log(task, stage, status, *, reason=None, user_id=None, group_id=None, **_):
        audit_logs.append((task, stage, status, reason))

    monkeypatch.setattr(cec, "request_with_retry", edge_failing_request)
    monkeypatch.setattr(cec, "emit_audit_log", fake_emit_audit_log)

    payload = {
        "title": "Lunch",
        "start_time": "2024-01-01T12:00:00Z",
        "invitees": ["bob"],
    }

    with pytest.raises(RuntimeError) as excinfo:
        dispatch("calendar.event.create_request", payload, user_id="alice")

    error_reason = str(excinfo.value)

    assert (
        "calendar.event.create",
        "persistence",
        "error",
        error_reason,
    ) in audit_logs
