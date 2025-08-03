from task_cascadence.workflows import dispatch
from task_cascadence.workflows import calendar_event_creation as cec


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


    emitted = {}

    def fake_emit(name, stage, user_id=None, group_id=None, **_kwargs):
        emitted["event"] = (name, stage, user_id, group_id)

    def fake_research(query, user_id=None, group_id=None):
        emitted["research"] = (query, user_id, group_id)

        return {"duration": "15m"}

    monkeypatch.setattr(cec, "request_with_retry", fake_request)
    monkeypatch.setattr(cec, "emit_stage_update_event", fake_emit)
    monkeypatch.setattr(cec.research, "async_gather", fake_async_gather)

    payload = {
        "title": "Lunch",
        "start_time": "2024-01-01T12:00:00Z",
        "location": "Cafe",
        "group_id": "g1",
        "invitees": ["bob"],
    }

    result = dispatch(
        "calendar.event.create_request", payload, user_id="alice", base_url="http://svc"
    )

    assert result == {"event_id": "evt1", "related_event_id": "evt2"}
    # permission checks
    assert calls[0][0] == "GET"
    assert "permissions" in calls[0][1]
    assert calls[1][0] == "POST"
    assert calls[1][1] == "http://svc/v1/calendar/events"
    assert calls[1][2]["json"]["travel_time"] == {"duration": "15m"}
    assert emitted["event"] == ("calendar.event.created", "created", "alice")
    assert emitted["research"] == ("travel time to Cafe", "alice", None)

