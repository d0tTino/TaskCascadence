import yaml
import pytest
from apscheduler.triggers.cron import CronTrigger
from task_cascadence.scheduler import CronScheduler


class DummyTask:
    def run(self) -> None:
        pass


def test_schedule_from_event_registers_metadata(tmp_path):
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    task = DummyTask()
    event = {"recurrence": {"cron": "*/5 * * * *"}}
    sched.schedule_from_event(
        task, event, user_id="alice", group_id="engineering"
    )
    job = sched.scheduler.get_job("DummyTask")
    assert job is not None
    expected = CronTrigger.from_crontab("*/5 * * * *", timezone="UTC")
    assert str(job.trigger) == str(expected)
    entry = sched.schedules["DummyTask"]
    assert entry["recurrence"] == {"cron": "*/5 * * * *"}
    assert entry["user_id"] == "alice"
    assert entry["group_id"] == "engineering"


def test_yaml_recurrence_loaded(tmp_path):
    data = {
        "DummyTask": {"expr": "* * * * *", "recurrence": {"note": "every minute"}}
    }
    cfg = tmp_path / "config.yml"
    cfg.write_text(yaml.safe_dump(data))
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    task = DummyTask()
    sched.load_yaml(cfg, {"DummyTask": task})
    assert sched.schedules["DummyTask"]["recurrence"] == {"note": "every minute"}


def test_schedule_from_event_missing_cron(tmp_path):
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    task = DummyTask()
    event = {"recurrence": {}}
    with pytest.raises(ValueError, match="event missing recurrence cron"):
        sched.schedule_from_event(task, event)
