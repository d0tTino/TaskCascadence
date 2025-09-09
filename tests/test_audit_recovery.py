from task_cascadence.audit_recovery import recover_partial
from task_cascadence.stage_store import StageStore
from task_cascadence.ume import _hash_user_id


def test_recover_partial_payload(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "stages.yml"))
    store = StageStore()

    user_id = "alice"
    group_id = "g1"
    user_hash = _hash_user_id(user_id)

    store.add_event(
        "ExampleTask",
        "research",
        user_hash=user_hash,
        group_id=group_id,
        partial=["step1"],
        category="audit",
    )

    recovered = recover_partial("ExampleTask", "research", user_id=user_id, group_id=group_id)
    assert recovered == ["step1"]

    recovered.append("step2")
    assert recovered == ["step1", "step2"]
