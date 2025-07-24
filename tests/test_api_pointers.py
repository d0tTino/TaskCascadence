import yaml
from fastapi.testclient import TestClient

from task_cascadence.api import app
from task_cascadence.scheduler import BaseScheduler
from task_cascadence.plugins import PointerTask


class DemoPointer(PointerTask):
    name = "demo_pointer"


def setup_scheduler(monkeypatch, tmp_path):
    sched = BaseScheduler()
    task = DemoPointer()
    sched.register_task("demo_pointer", task)
    monkeypatch.setattr("task_cascadence.api.get_default_scheduler", lambda: sched)
    return sched, task


def test_api_pointer_add_and_list(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(tmp_path / "pointers.yml"))

    sched, _task = setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)

    resp = client.post(
        "/pointers/demo_pointer/add",
        params={"user_id": "alice", "run_id": "run1"},
    )
    assert resp.status_code == 200

    resp = client.get("/pointers/demo_pointer")
    assert resp.status_code == 200
    data = resp.json()

    stored = yaml.safe_load((tmp_path / "pointers.yml").read_text())
    user_hash = stored["demo_pointer"][0]["user_hash"]
    assert user_hash != "alice"
    assert data == [{"run_id": "run1", "user_hash": user_hash}]


def test_api_pointer_send(monkeypatch):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    sent = {}

    def fake_emit(update):
        sent["update"] = update

    monkeypatch.setattr("task_cascadence.api.emit_pointer_update", fake_emit)
    client = TestClient(app)

    resp = client.post(
        "/pointers/demo_pointer/send",
        params={"user_id": "alice", "run_id": "r1"},
    )
    assert resp.status_code == 200
    update = sent["update"]
    assert update.task_name == "demo_pointer"
    assert update.run_id == "r1"
    assert update.user_hash != "alice"


def test_api_pointer_receive(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(tmp_path / "pointers.yml"))

    client = TestClient(app)
    resp = client.post(
        "/pointers/demo_pointer/receive",
        params={"run_id": "r2", "user_hash": "xyz"},
    )
    assert resp.status_code == 200

    data = yaml.safe_load((tmp_path / "pointers.yml").read_text())
    assert data["demo_pointer"][0] == {"run_id": "r2", "user_hash": "xyz"}
