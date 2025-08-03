from task_cascadence.idea_store import IdeaStore
from task_cascadence.ume import _hash_user_id, IdeaSeed


def test_per_user_and_group_isolation(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    store = IdeaStore(path=tmp_path / "ideas.yml")
    u1 = _hash_user_id("alice")
    u2 = _hash_user_id("bob")

    store.add_seed(IdeaSeed(text="foo", user_hash=u1), group_id="g1")
    store.add_seed(IdeaSeed(text="bar", user_hash=u1), group_id="g2")
    store.add_seed(IdeaSeed(text="baz", user_hash=u2), group_id="g1")

    assert [s["text"] for s in store.get_seeds(u1, "g1")] == ["foo"]
    assert [s["text"] for s in store.get_seeds(u1, "g2")] == ["bar"]
    assert [s["text"] for s in store.get_seeds(u2, "g1")] == ["baz"]
    assert store.get_seeds(u2, "g2") == []
