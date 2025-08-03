from task_cascadence.suggestion_store import SuggestionStore
from task_cascadence.ume import _hash_user_id


def test_per_user_and_group_isolation(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    store = SuggestionStore(path=tmp_path / "suggestions.yml")
    u1 = _hash_user_id("alice")
    u2 = _hash_user_id("bob")

    store.add_decision("p1", "accept", u1, group_id="g1")
    store.add_decision("p2", "dismiss", u1, group_id="g2")
    store.add_decision("p3", "accept", u2, group_id="g1")

    assert [d["pattern"] for d in store.get_decisions(u1, "g1")] == ["p1"]
    assert [d["pattern"] for d in store.get_decisions(u1, "g2")] == ["p2"]
    assert [d["pattern"] for d in store.get_decisions(u2, "g1")] == ["p3"]
    assert store.get_decisions(u2, "g2") == []
