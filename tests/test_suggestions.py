import asyncio

from task_cascadence.suggestions.engine import SuggestionEngine


async def _prepare_engine(monkeypatch):
    engine = SuggestionEngine()

    async def fake_query():
        return [
            {
                "title": "Example",
                "description": "test description",
                "task_name": "dummy",
                "related": ["foo"],
            }
        ]

    monkeypatch.setattr(engine, "_query_ume", fake_query)
    monkeypatch.setattr(
        "task_cascadence.suggestions.engine.gather",
        lambda q: {"confidence": 0.9},
    )
    await engine.generate()
    return engine


def test_generation(monkeypatch):
    engine = asyncio.run(_prepare_engine(monkeypatch))
    suggestions = engine.list()
    assert suggestions
    s = suggestions[0]
    assert s.title == "Example"
    assert s.confidence == 0.9
    assert s.related_entities == ["foo"]


def test_snooze_and_dismiss(monkeypatch):
    engine = asyncio.run(_prepare_engine(monkeypatch))
    sid = engine.list()[0].id
    engine.snooze(sid)
    assert engine.get(sid).state == "snoozed"
    engine.dismiss(sid)
    assert engine.get(sid).state == "dismissed"


def test_accept_enqueues_task(monkeypatch):
    engine = asyncio.run(_prepare_engine(monkeypatch))
    sid = engine.list()[0].id

    class DummyScheduler:
        def __init__(self):
            self.ran = []

        def run_task(self, name):
            self.ran.append(name)

    scheduler = DummyScheduler()
    monkeypatch.setattr(
        "task_cascadence.suggestions.engine.get_default_scheduler", lambda: scheduler
    )

    emitted = {}

    def fake_emit(note, user_id=None):
        emitted["text"] = note.note
        emitted["user_id"] = user_id

    monkeypatch.setattr(
        "task_cascadence.suggestions.engine.emit_task_note", fake_emit
    )

    engine.accept(sid, user_id="alice")
    assert scheduler.ran == ["dummy"]
    assert emitted["user_id"] == "alice"
    assert "accepted" in emitted["text"]
    assert engine.get(sid).state == "accepted"
