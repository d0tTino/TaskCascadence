import yaml
from task_cascadence.scheduler import CronScheduler
from task_cascadence.plugins import CronTask


class DummyTask(CronTask):
    def __init__(self):
        self.count = 0

    def run(self):
        self.count += 1
        return self.count


def test_timezone_awareness(tmp_path):
    storage = tmp_path / "sched.yml"
    sched = CronScheduler(timezone="US/Pacific", storage_path=storage)
    task = DummyTask()
    sched.register_task(task, "0 12 * * *")
    job = sched.scheduler.get_job("DummyTask")
    assert str(job.trigger.timezone) == "US/Pacific"


def test_schedule_persistence(tmp_path):
    storage = tmp_path / "sched.yml"
    sched = CronScheduler(timezone="UTC", storage_path=storage)
    task = DummyTask()
    sched.register_task(task, "*/5 * * * *")
    data = yaml.safe_load(storage.read_text())
    assert data["DummyTask"] == "*/5 * * * *"


def test_run_emits_result(monkeypatch, tmp_path):
    emitted = {}

    def fake_emit(data):
        emitted.update(data)

    from task_cascadence import ume

    monkeypatch.setattr(ume, "emit_task_run", fake_emit)
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    task = DummyTask()
    sched.register_task(task, "*/1 * * * *")
    job = sched.scheduler.get_job("DummyTask")
    job.func()
    assert emitted["task"] == "DummyTask"
    assert emitted["result"] == 1


def test_restore_schedules_on_init(tmp_path, monkeypatch):
    storage = tmp_path / "sched.yml"
    task = DummyTask()
    sched = CronScheduler(timezone="UTC", storage_path=storage)
    sched.register_task(task, "*/5 * * * *")

    from task_cascadence import ume

    monkeypatch.setattr(ume, "emit_task_run", lambda data: None)

    new_task = DummyTask()
    sched2 = CronScheduler(
        timezone="UTC", storage_path=storage, tasks={"DummyTask": new_task}
    )
    job = sched2.scheduler.get_job("DummyTask")
    assert job is not None
    job.func()
    assert new_task.count == 1

