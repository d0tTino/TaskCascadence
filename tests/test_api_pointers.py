from task_cascadence.api import app
from task_cascadence.scheduler import BaseScheduler
from task_cascadence.plugins import PointerTask
from task_cascadence.ume import _hash_user_id
from fastapi.testclient import TestClient
from types import SimpleNamespace
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
    headers = {"X-User-ID": "alice", "X-Group-ID": "team-a"}
    resp = client.post(
        "/pointers/demo_pointer",
        params={"run_id": "r1"},
        headers=headers,
    )
    assert resp.status_code == 200
    assert resp.json() == {"status": "added"}


    resp = client.get("/pointers/demo_pointer", headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data == [
        {
            "run_id": "r1",
            "user_hash": _hash_user_id("alice"),
            "group_id": "team-a",
        }
    ]
    pointer = task.get_pointers()[0]
    assert pointer.run_id == "r1"
    assert pointer.group_id == "team-a"


def test_pointer_receive(monkeypatch):
    sched, task, store_path = setup_scheduler(monkeypatch)
    client = TestClient(app)
    user_hash = _hash_user_id("bob")
    resp = client.post(
        "/pointers/demo_pointer/receive",
        params={"run_id": "r2", "user_hash": user_hash},
        headers={"X-User-ID": "bob", "X-Group-ID": "team-b"},
    )
    assert resp.status_code == 200
    assert resp.json() == {"status": "stored"}

    data = yaml.safe_load(open(store_path).read())
    assert data["demo_pointer"] == [
        {"run_id": "r2", "user_hash": user_hash, "group_id": "team-b"}
    ]


def test_pointer_headers_required(monkeypatch):
    setup_scheduler(monkeypatch)
    client = TestClient(app)

    resp = client.post(
        "/pointers/demo_pointer",
        params={"run_id": "r1"},
        headers={"X-Group-ID": "team-a"},
    )
    assert resp.status_code == 400

    resp = client.get(
        "/pointers/demo_pointer",
    )
    assert resp.status_code == 400

    resp = client.post(
        "/pointers/demo_pointer/receive",
        params={"run_id": "r2", "user_hash": _hash_user_id("bob")},
        headers={"X-User-ID": "bob"},
    )
    assert resp.status_code == 400


def test_suggestion_list_requires_scope(monkeypatch):
    client = TestClient(app)

    class DummyEngine:
        def __init__(self):
            self.calls = []

        def list(self, user_id=None, group_id=None):
            self.calls.append((user_id, group_id))
            return [
                SimpleNamespace(
                    id="s1",
                    title="demo",
                    description="desc",
                    confidence=0.5,
                    related_entities=[],
                    task_name=None,
                    context={},
                    state="pending",
                    user_id=user_id,
                    group_id=group_id,
                )
            ]

    engine = DummyEngine()
    monkeypatch.setattr("task_cascadence.api.get_default_engine", lambda: engine)

    resp = client.get("/suggestions", headers={"X-Group-ID": "team"})
    assert resp.status_code == 400

    resp = client.get("/suggestions", headers={"X-User-ID": "alice"})
    assert resp.status_code == 400

    headers = {"X-User-ID": "alice", "X-Group-ID": "team"}
    resp = client.get("/suggestions", headers=headers)
    assert resp.status_code == 200
    assert engine.calls == [("alice", "team")]
    assert resp.json() == [
        {
            "confidence": 0.5,
            "context": {},
            "description": "desc",
            "group_id": "team",
            "id": "s1",
            "related_entities": [],
            "state": "pending",
            "task_name": None,
            "title": "demo",
            "user_id": "alice",
        }
    ]

