from task_cascadence.api import app
from task_cascadence.scheduler import BaseScheduler
from task_cascadence.plugins import PointerTask
from task_cascadence.ume import _hash_user_id
from fastapi.testclient import TestClient
import tempfile
import yaml

class DemoPointer(PointerTask):
    name = "demo_pointer"


def setup_scheduler(monkeypatch):
    sched = BaseScheduler()
    tmp = tempfile.NamedTemporaryFile(delete=False)
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", tmp.name)
    task = DemoPointer()
    sched.register_task("demo_pointer", task)
    monkeypatch.setattr("task_cascadence.api.get_default_scheduler", lambda: sched)
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)
    return sched, task, tmp.name


def test_pointer_add_and_list(monkeypatch):
    sched, task, store = setup_scheduler(monkeypatch)
    client = TestClient(app)
    resp = client.post("/pointers/demo_pointer", params={"user_id": "alice", "run_id": "r1"})
    assert resp.status_code == 200
    assert resp.json() == {"status": "added"}

    resp = client.get("/pointers/demo_pointer")
    assert resp.status_code == 200
    data = resp.json()
    assert data == [{"run_id": "r1", "user_hash": _hash_user_id("alice")}] 
    assert task.get_pointers()[0].run_id == "r1"


def test_pointer_receive(monkeypatch):
    sched, task, store_path = setup_scheduler(monkeypatch)
    client = TestClient(app)
    user_hash = _hash_user_id("bob")
    resp = client.post(
        "/pointers/demo_pointer/receive",
        params={"run_id": "r2", "user_hash": user_hash},
    )
    assert resp.status_code == 200
    assert resp.json() == {"status": "stored"}

    data = yaml.safe_load(open(store_path).read())
    assert data["demo_pointer"] == [{"run_id": "r2", "user_hash": user_hash}]
