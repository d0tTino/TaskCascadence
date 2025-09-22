from apscheduler.triggers.cron import CronTrigger
from task_cascadence.scheduler import CronScheduler


class DummyTask:
    def run(self) -> None:  # pragma: no cover - simple noop for scheduling
        pass


def test_poll_calendar_event(monkeypatch, tmp_path):
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    task = DummyTask()
    called: dict[str, str | None] = {}

    def fake_fetch(self, node: str, *, user_id: str | None = None, group_id: str | None = None):
        called["user_id"] = user_id
        called["group_id"] = group_id
        return {"recurrence": {"cron": "*/5 * * * *"}}

    monkeypatch.setattr(CronScheduler, "_fetch_calendar_event", fake_fetch)

    sched.poll_calendar_event(task, "node42", interval=1, user_id="alice", group_id="engineering")

    assert called["user_id"] == "alice"
    assert called["group_id"] == "engineering"
    job = sched.scheduler.get_job("DummyTask")
    assert job is not None
    expected = CronTrigger.from_crontab("*/5 * * * *", timezone="UTC")
    assert str(job.trigger) == str(expected)
    entry = sched.schedules["DummyTask"]
    assert entry["recurrence"] == {"cron": "*/5 * * * *"}
    assert entry["calendar_event"] == {"node": "node42", "poll": 1}
    from task_cascadence.ume import _hash_user_id

    assert entry["user_hash"] == _hash_user_id("alice")
    assert entry["group_hash"] == _hash_user_id("engineering")
