import asyncio

import pytest
from task_cascadence.suggestions.engine import SuggestionEngine


async def _prepare_engine(monkeypatch, events=None, tmp_path=None):
    if tmp_path is not None:
        monkeypatch.setenv(
            "CASCADENCE_SUGGESTIONS_PATH", str(tmp_path / "decisions.yml")
        )
    engine = SuggestionEngine()

    async def fake_query(user_id=None, group_id=None):
        return events or [
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
        lambda q, user_id, group_id=None: {"confidence": 0.9},
    )
    await engine.generate(user_id="alice")
    return engine


def test_generation(monkeypatch, tmp_path):
    engine = asyncio.run(_prepare_engine(monkeypatch, tmp_path=tmp_path))
    suggestions = engine.list()
    assert suggestions
    s = suggestions[0]
    assert s.title == "Example"
    assert s.confidence == 0.9
    assert s.related_entities == ["foo"]


def test_snooze_and_dismiss(monkeypatch, tmp_path):
    engine = asyncio.run(_prepare_engine(monkeypatch, tmp_path=tmp_path))
    sid = engine.list()[0].id
    engine.snooze(sid)
    assert engine.get(sid).state == "snoozed"
    engine.dismiss(sid)
    assert engine.get(sid).state == "dismissed"


@pytest.mark.skip(reason="requires transport client configuration")
def test_accept_enqueues_task(monkeypatch, tmp_path):
    engine = asyncio.run(_prepare_engine(monkeypatch, tmp_path=tmp_path))
    sid = engine.list()[0].id

    class DummyScheduler:
        def __init__(self):
            self.ran = []

        def run_task(self, name, user_id=None, group_id=None):
            self.ran.append(name)

    scheduler = DummyScheduler()
    monkeypatch.setattr(
        "task_cascadence.suggestions.engine.get_default_scheduler", lambda: scheduler
    )

    emitted = {}

    def fake_emit(note, client=None, user_id=None, use_asyncio=False):
        emitted["text"] = note.note
        emitted["user_id"] = user_id

    monkeypatch.setattr(
        "task_cascadence.ume.emit_task_note", fake_emit
    )

    engine.accept(sid, user_id="alice")
    assert scheduler.ran == ["dummy"]
    assert emitted["user_id"] == "alice"
    assert emitted["text"] == "accepted suggestion"
    assert engine.get(sid).state == "accepted"


def test_private_events_excluded(monkeypatch, tmp_path):
    events = [
        {
            "title": "Public",
            "description": "ok",
            "task_name": "dummy",
        },
        {
            "title": "Secret",
            "description": "hidden",
            "task_name": "dummy",
            "privacy": "private",
        },
        {
            "title": "Sensitive",
            "description": "hidden",
            "task_name": "dummy",
            "flags": ["sensitive"],
        },
    ]
    engine = asyncio.run(_prepare_engine(monkeypatch, events=events, tmp_path=tmp_path))
    suggestions = engine.list()
    assert len(suggestions) == 1
    assert suggestions[0].title == "Public"
