import yaml
from task_cascadence.scheduler import CronScheduler


class DummyTask:
    def __init__(self) -> None:
        self.count = 0

    def run(self) -> None:
        self.count += 1


def test_load_yaml_schedule(tmp_path):
    data = {
        "DummyTask": {
            "expr": "* * * * *",
            "recurrence": {"note": "every minute"},
        }
    }
    cfg = tmp_path / "config.yml"
    cfg.write_text(yaml.safe_dump(data))
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    task = DummyTask()
    sched.load_yaml(cfg, {"DummyTask": task})
    job = sched.scheduler.get_job("DummyTask")
    assert job is not None
    assert sched.schedules["DummyTask"]["recurrence"] == {"note": "every minute"}


def test_schedule_from_calendar_event(tmp_path):
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    task = DummyTask()
    event = {"title": "review", "recurrence": {"cron": "*/2 * * * *"}}
    sched.schedule_from_event(task, event)
    job = sched.scheduler.get_job("DummyTask")
    assert job is not None
    assert sched.schedules["DummyTask"]["recurrence"] == {"cron": "*/2 * * * *"}
