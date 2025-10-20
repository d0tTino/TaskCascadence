import tempfile
from fastapi.testclient import TestClient
from task_cascadence.api import app
from task_cascadence.scheduler import CronScheduler
from task_cascadence.plugins import CronTask


class DummyTask(CronTask):
    name = "dummy"

    def run(self):
        return "ok"

def setup_scheduler(monkeypatch, tmp_path):
    sched = CronScheduler(storage_path=tmp_path / "sched.yml")
    task = DummyTask()
    sched.register_task(name_or_task="dummy", task_or_expr=task)
    monkeypatch.setattr("task_cascadence.api.get_default_scheduler", lambda: sched)
    monkeypatch.setattr(
        "task_cascadence.ume.emit_task_run",
        lambda run, user_id=None, group_id=None: None,
    )
    tmp = tempfile.NamedTemporaryFile(delete=False)
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", tmp.name)
    monkeypatch.setenv("CASCADENCE_TASKS_PATH", str(tmp_path / "tasks.yml"))
    return sched


def test_register_task_bad_format(monkeypatch, tmp_path):
    setup_scheduler(monkeypatch, tmp_path)
    client = TestClient(app)
    resp = client.post(
        "/tasks",
        params={"path": "bad:format:extra"},
        headers={"X-User-ID": "alice", "X-Group-ID": "team"},
    )
    assert resp.status_code == 400
    assert "module:Class" in resp.json()["detail"]
