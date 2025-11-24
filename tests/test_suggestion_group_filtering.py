import asyncio

import task_cascadence.ume as ume
from task_cascadence.suggestions.engine import SuggestionEngine
from task_cascadence.ume.models import IdeaSeed


def test_generate_filters_by_user_and_group(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_IDEAS_PATH", str(tmp_path / "ideas.yml"))
    monkeypatch.setenv("CASCADENCE_SUGGESTIONS_PATH", str(tmp_path / "decisions.yml"))
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    ume._idea_store = None
    ume._suggestion_store = None

    idea_store = ume._get_idea_store()
    alice_hash = ume._hash_user_id("alice")
    idea_store.add_seed(IdeaSeed(text="foo", user_hash=alice_hash), group_id="group1")
    idea_store.add_seed(IdeaSeed(text="foo", user_hash=alice_hash), group_id="group1")
    idea_store.add_seed(IdeaSeed(text="foo", user_hash=alice_hash), group_id="group2")
    idea_store.add_seed(IdeaSeed(text="foo", user_hash=alice_hash), group_id="group2")

    bob_hash = ume._hash_user_id("bob")
    idea_store.add_seed(IdeaSeed(text="bar", user_hash=bob_hash), group_id="group1")
    idea_store.add_seed(IdeaSeed(text="bar", user_hash=bob_hash), group_id="group1")

    engine = SuggestionEngine()
    monkeypatch.setattr(
        "task_cascadence.suggestions.engine.gather",
        lambda q, user_id, group_id=None: {"confidence": 0.5},
    )

    asyncio.run(engine.generate(user_id="alice", group_id="group1"))
    suggestions = engine.list(user_id="alice", group_id="group1")

    assert len(suggestions) == 1
    assert suggestions[0].title == "Repeated bookmark: foo"
    assert suggestions[0].group_id == "group1"
