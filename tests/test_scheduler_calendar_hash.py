from task_cascadence.scheduler import CronScheduler
import task_cascadence.scheduler as scheduler
from task_cascadence.ume import _hash_user_id


def expected_hash(value: str) -> str:
    return _hash_user_id(value)


class DummyResp:
    def json(self) -> dict:
        return {}


def test_fetch_calendar_event_hashes_identifiers(monkeypatch, tmp_path):
    captured: dict[str, dict | None] = {}

    def fake_request(method, url, timeout=0, **kwargs):
        captured["params"] = kwargs.get("params")
        return DummyResp()

    monkeypatch.setattr(scheduler, "request_with_retry", fake_request)
    monkeypatch.setenv("UME_BASE_URL", "http://ume")
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "s.yml")
    sched._fetch_calendar_event("foo", user_id="alice", group_id="engineering")

    assert captured["params"]["user_id"] == expected_hash("alice")
    assert captured["params"]["group_id"] == expected_hash("engineering")


def test_register_task_hashes_group_id(tmp_path):
    class DummyTask:
        def run(self):
            pass

    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "s.yml")
    task = DummyTask()
    sched.register_task(task, "* * * * *", user_id="alice", group_id="engineering")
    entry = sched.schedules["DummyTask"]
    assert entry["group_id"] == expected_hash("engineering")
    import yaml

    persisted = yaml.safe_load((tmp_path / "s.yml").read_text())
    assert persisted["DummyTask"]["group_id"] == expected_hash("engineering")
