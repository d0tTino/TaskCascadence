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

    async def fake_async_gather(query):
        emitted["research"] = query
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
    assert calls[1][0] == "GET" and calls[1][2]["params"]["group_id"] == "g1"
    assert calls[2][0] == "GET" and calls[2][2]["params"]["invitee"] == "bob"
    # main event persisted
    assert calls[3][0] == "POST"
    assert calls[3][1] == "http://svc/v1/calendar/events"
    assert calls[3][2]["json"]["user_id"] == "alice"
    assert calls[3][2]["json"]["group_id"] == "g1"
    assert calls[3][2]["json"]["travel_time"] == {"duration": "15m"}
    # related event persisted
    assert calls[4][0] == "POST" and calls[4][1] == "http://svc/v1/calendar/events"
    # edge creation
    assert calls[5][0] == "POST" and calls[5][1] == "http://svc/v1/calendar/edges"
    assert calls[5][2]["json"]["type"] == "RELATES_TO"
    assert emitted["event"] == ("calendar.event.created", "created", "alice", "g1")
    assert emitted["research"] == "travel time to Cafe"
