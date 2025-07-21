from task_cascadence.plugins import PointerTask
from task_cascadence.ume.models import TaskPointer


def test_pointer_creation_and_retrieval(monkeypatch):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    task = PointerTask()
    task.add_pointer("alice", "run1")
    task.add_pointer("bob", "run2")

    pointers = task.get_pointers()
    assert [p.run_id for p in pointers] == ["run1", "run2"]
    assert all(isinstance(p, TaskPointer) for p in pointers)
    assert pointers[0].user_hash != "alice"
    assert pointers[1].user_hash != "bob"
