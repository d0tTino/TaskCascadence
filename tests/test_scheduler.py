import yaml
from task_cascadence.scheduler import CronScheduler, BaseScheduler
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
    from zoneinfo import ZoneInfo

    assert str(job.trigger.timezone) == "US/Pacific"
    assert isinstance(job.trigger.timezone, ZoneInfo)


def test_schedule_persistence(tmp_path):
    storage = tmp_path / "sched.yml"
    sched = CronScheduler(timezone="UTC", storage_path=storage)
    task = DummyTask()
    sched.register_task(task, "*/5 * * * *")
    data = yaml.safe_load(storage.read_text())
    assert data["DummyTask"] == "*/5 * * * *"


def test_run_emits_result(monkeypatch, tmp_path):
    emitted_run = None

    def fake_emit(run):
        nonlocal emitted_run
        emitted_run = run

    from task_cascadence import ume

    monkeypatch.setattr(ume, "emit_task_run", fake_emit)
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    task = DummyTask()
    sched.register_task(task, "*/1 * * * *")
    job = sched.scheduler.get_job("DummyTask")
    job.func()
    assert emitted_run is not None
    assert emitted_run.spec.id == "DummyTask"
    assert emitted_run.status == "success"
    assert task.count == 1


def test_restore_schedules_on_init(tmp_path, monkeypatch):
    storage = tmp_path / "sched.yml"
    task = DummyTask()
    sched = CronScheduler(timezone="UTC", storage_path=storage)
    sched.register_task(task, "*/5 * * * *")

    from task_cascadence import ume

    monkeypatch.setattr(ume, "emit_task_run", lambda run: None)

    new_task = DummyTask()
    sched2 = CronScheduler(
        timezone="UTC", storage_path=storage, tasks={"DummyTask": new_task}
    )
    job = sched2.scheduler.get_job("DummyTask")
    assert job is not None
    job.func()
    assert new_task.count == 1


def test_schedule_task(tmp_path):
    storage = tmp_path / "sched.yml"
    sched = CronScheduler(timezone="UTC", storage_path=storage)
    task = DummyTask()
    sched.schedule_task(task, "*/2 * * * *")
    job = sched.scheduler.get_job("DummyTask")
    assert job is not None
    data = yaml.safe_load(storage.read_text())
    assert data["DummyTask"] == "*/2 * * * *"


def test_base_scheduler_has_no_schedule_task():
    bs = BaseScheduler()
    assert not hasattr(bs, "schedule_task")


def test_metrics_increment_for_job(tmp_path, monkeypatch):
    from task_cascadence import metrics

    storage = tmp_path / "sched.yml"
    sched = CronScheduler(timezone="UTC", storage_path=storage)
    task = DummyTask()

    # Prevent actual event emission
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda run: None)

    sched.register_task(task, "*/1 * * * *")
    job = sched.scheduler.get_job("DummyTask")

    success = metrics.TASK_SUCCESS.labels("runner")
    failure = metrics.TASK_FAILURE.labels("runner")

    before_success = success._value.get()
    before_failure = failure._value.get()

    job.func()

    assert success._value.get() == before_success + 1
    assert failure._value.get() == before_failure

