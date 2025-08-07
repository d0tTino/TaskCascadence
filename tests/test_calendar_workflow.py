
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
    monkeypatch.setattr(cec, "emit_task_note", fake_emit_note)
    monkeypatch.setattr(cec.research, "async_gather", fake_async_gather)
    monkeypatch.setattr(cec.research, "gather", fake_gather)
    monkeypatch.setattr(cec.research, "async_gather", fake_async_gather)
    monkeypatch.setattr(cec, "emit_task_note", lambda *a, **kw: None)

    def fake_dispatch(event, data, *, user_id=None, group_id=None):
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


