from datetime import datetime, timedelta, timezone

import task_cascadence.ume as ume
from task_cascadence.ume.models import IdeaSeed


def test_detect_repeated_bookmarks(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_IDEAS_PATH", str(tmp_path / "ideas.yml"))
    monkeypatch.setenv("CASCADENCE_SUGGESTIONS_PATH", str(tmp_path / "decisions.yml"))
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    ume._idea_store = None
    ume._suggestion_store = None

    store = ume._get_idea_store()
    seed = IdeaSeed(text="foo", user_hash=ume._hash_user_id("alice"))
    store.add_seed(seed)
    store.add_seed(seed)

    patterns = ume.detect_event_patterns(user_id="alice")
    assert patterns
    assert patterns[0]["context"]["count"] == 2


def test_dismissed_suggestions_suppressed(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_IDEAS_PATH", str(tmp_path / "ideas.yml"))
    monkeypatch.setenv("CASCADENCE_SUGGESTIONS_PATH", str(tmp_path / "decisions.yml"))
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    ume._idea_store = None
    ume._suggestion_store = None

    store = ume._get_idea_store()
    seed = IdeaSeed(text="bar", user_hash=ume._hash_user_id("alice"))
    store.add_seed(seed)
    store.add_seed(seed)
    ume.record_suggestion_decision(
        "Repeated bookmark: bar", "dismissed", user_id="alice"
    )

    patterns = ume.detect_event_patterns(user_id="alice")
    assert patterns == []

    decision_store = ume._get_suggestion_store()
    decision_store._data[0]["time"] = (
        datetime.now(timezone.utc) - timedelta(days=31)
    ).isoformat()
    decision_store._save()
    patterns = ume.detect_event_patterns(user_id="alice")
    assert patterns

