import json

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

    def fake_stage(name, stage, user_id=None, group_id=None):
        emitted["stage"] = (name, stage, user_id, group_id)

    def fake_note(note, user_id=None, group_id=None):
        emitted["note"] = (note, user_id, group_id)

    async def fake_async_gather(query, user_id=None, group_id=None):
        emitted["research"] = (query, user_id, group_id)
        return {"duration": "15m"}

    monkeypatch.setattr(cec, "request_with_retry", fake_request)
    monkeypatch.setattr(cec, "emit_stage_update_event", fake_stage)
    monkeypatch.setattr(cec, "emit_task_note", fake_note)
    monkeypatch.setattr(cec.research, "async_gather", fake_async_gather)

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
    assert any(c[0] == "GET" and "permissions" in c[1] for c in calls)
    post_event = [c for c in calls if c[0] == "POST" and c[1] == "http://svc/v1/calendar/events"][0]
    assert post_event[2]["json"]["travel_time"] == {"duration": "15m"}

    invited_edges = [c for c in calls if c[1].endswith("/edges") and c[2]["json"]["type"] == "INVITED"]
    assert invited_edges and invited_edges[0][2]["json"]["dst"] == "bob"
    layer_edges = [c for c in calls if c[1].endswith("/edges") and c[2]["json"]["type"] == "LAYER"]
    assert layer_edges and layer_edges[0][2]["json"]["dst"] == "work"

    assert emitted["stage"] == ("calendar.event.created", "created", "alice", "g1")

    note, note_user, note_group = emitted["note"]
    assert note.task_name == "calendar.event.created"
    data = json.loads(note.note)
    assert data["event_id"] == "evt1"
    assert data["related_event_id"] == "evt2"
    assert note_user == "alice"
    assert note_group == "g1"
    assert emitted["research"] == ("travel time to Cafe", "alice", "g1")
