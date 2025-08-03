from task_cascadence.workflows import dispatch
from task_cascadence.workflows import calendar_event_creation as cec


class DummyResponse:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


def test_calendar_event_creation(monkeypatch):
    calls = []

    def fake_request(method, url, timeout, **kwargs):
        calls.append((method, url, kwargs))
        if method == "GET":
            return DummyResponse({"allowed": True})
        assert method == "POST"
        return DummyResponse({"ok": True})

    emitted = {}

    def fake_emit(name, stage, user_id=None, group_id=None, **_kwargs):
        emitted["event"] = (name, stage, user_id, group_id)

    research_ctx = {}

    def fake_research(query, *, user_id=None, group_id=None):
        research_ctx["called"] = (query, user_id, group_id)
        return {"duration": "15m"}

    monkeypatch.setattr(cec, "request_with_retry", fake_request)
    monkeypatch.setattr(cec, "emit_stage_update_event", fake_emit)
    monkeypatch.setattr(cec.research, "gather", fake_research)

    payload = {
        "title": "Lunch",
        "start": "2024-01-01T12:00:00Z",
        "end": "2024-01-01T13:00:00Z",
        "location": "Cafe",
    }

    result = dispatch(
        "calendar.event.request",
        payload,
        user_id="alice",
        group_id="devs",
        base_url="http://svc",
    )

    assert result == {"ok": True}
    # permission check
    assert calls[0][0] == "GET"
    assert calls[0][2]["params"]["user_id"] == "alice"
    assert calls[0][2]["params"]["group_id"] == "devs"
    # persistence call
    assert calls[1][0] == "POST"
    assert calls[1][1] == "http://svc/v1/calendar/events"
    post_json = calls[1][2]["json"]
    assert any(n["title"].startswith("Leave by") for n in post_json["nodes"])
    assert {"reason": "travel"} == post_json["edges"][0]["data"]
    assert post_json["edges"][0]["type"] == "RELATES_TO"
    # user/group context propagation
    assert research_ctx["called"] == ("travel time to Cafe", "alice", "devs")
    assert emitted["event"] == ("calendar.event.created", "created", "alice", "devs")


def test_calendar_event_requires_location(monkeypatch):
    def fake_perm(*args, **kwargs):
        return True

    monkeypatch.setattr(cec, "_has_permission", fake_perm)

    payload = {
        "title": "Lunch",
        "start": "2024-01-01T12:00:00Z",
        "end": "2024-01-01T13:00:00Z",
    }

    try:
        dispatch("calendar.event.request", payload, user_id="alice")
    except ValueError as exc:
        assert "location" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("expected ValueError for missing location")
