import yaml
import pytest
import requests
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
    sched.register_task(name_or_task=task, task_or_expr="0 12 * * *")
    job = sched.scheduler.get_job("DummyTask")
    from zoneinfo import ZoneInfo

    assert str(job.trigger.timezone) == "US/Pacific"
    assert isinstance(job.trigger.timezone, ZoneInfo)


def test_env_timezone_applied_on_initialize(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_TIMEZONE", "US/Pacific")
    import importlib
    import task_cascadence

    monkeypatch.setattr(task_cascadence.plugins, "initialize", lambda: None)
    monkeypatch.setattr(task_cascadence.plugins, "load_cronyx_tasks", lambda: None)

    importlib.reload(task_cascadence)
    task_cascadence.initialize()

    from task_cascadence.scheduler import get_default_scheduler

    sched = get_default_scheduler()
    from zoneinfo import ZoneInfo

    assert str(sched.scheduler.timezone) == "US/Pacific"

    sched.storage_path = tmp_path / "sched.yml"
    task = DummyTask()
    sched.register_task(name_or_task=task, task_or_expr="0 6 * * *")
    job = sched.scheduler.get_job("DummyTask")

    assert str(job.trigger.timezone) == "US/Pacific"
    assert isinstance(job.trigger.timezone, ZoneInfo)


def test_schedule_persistence(tmp_path):
    storage = tmp_path / "sched.yml"
    sched = CronScheduler(timezone="UTC", storage_path=storage)
    task = DummyTask()
    sched.register_task(name_or_task=task, task_or_expr="*/5 * * * *", user_id="alice")
    data = yaml.safe_load(storage.read_text())
    from task_cascadence.ume import _hash_user_id

    assert data["DummyTask"]["expr"] == "*/5 * * * *"
    assert data["DummyTask"]["user_hash"] == _hash_user_id("alice")


def test_run_emits_result(monkeypatch, tmp_path):
    emitted_run = None

    from task_cascadence.ume import _hash_user_id

    def fake_emit(run, user_id=None):
        nonlocal emitted_run
        if user_id is not None:
            run.user_hash = _hash_user_id(user_id)
        emitted_run = run

    from task_cascadence import ume

    monkeypatch.setattr(ume, "emit_task_run", fake_emit)
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    task = DummyTask()
    sched.register_task(name_or_task=task, task_or_expr="*/1 * * * *", user_id="alice")
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
    sched.register_task(name_or_task=task, task_or_expr="*/5 * * * *", user_id="alice")

    from task_cascadence import ume

    captured: dict[str, str | None] = {}

    from task_cascadence.ume import _hash_user_id

    def fake_emit(run, user_id=None):
        captured["user_id"] = user_id

    monkeypatch.setattr(ume, "emit_task_run", fake_emit)

    new_task = DummyTask()
    sched2 = CronScheduler(
        timezone="UTC", storage_path=storage, tasks={"DummyTask": new_task}
    )
    job = sched2.scheduler.get_job("DummyTask")
    assert job is not None
    job.func()
    assert new_task.count == 1
    assert captured["user_id"] == "alice"


def test_restored_scheduler_preserves_plain_identifiers(tmp_path, monkeypatch):
    from task_cascadence import scheduler as scheduler_module

    captured: list[dict[str, object]] = []

    class Resp:
        def json(self) -> dict[str, object]:  # pragma: no cover - compatibility
            return {}

    def fake_request(method, url, timeout=0, **kwargs):
        captured.append({"method": method, "url": url, "kwargs": kwargs})
        return Resp()

    monkeypatch.setattr(scheduler_module, "request_with_retry", fake_request)

    class HttpTask:
        def __init__(self) -> None:
            self.calls = 0

        def run(self) -> None:
            self.calls += 1
            scheduler_module.request_with_retry(
                "POST",
                "http://ume/test",
                json={
                    "user_id": self.user_id,
                    "group_id": self.group_id,
                },
            )

    storage = tmp_path / "sched.yml"
    sched = CronScheduler(timezone="UTC", storage_path=storage)
    task = HttpTask()
    sched.register_task(
        name_or_task=task,
        task_or_expr="*/5 * * * *",
        user_id="alice",
        group_id="engineering",
    )

    restored_task = HttpTask()
    sched2 = CronScheduler(
        timezone="UTC",
        storage_path=storage,
        tasks={"HttpTask": restored_task},
    )

    job = sched2.scheduler.get_job("HttpTask")
    assert job is not None

    captured.clear()
    job.func()

    assert restored_task.calls == 1
    assert captured
    payload = captured[-1]["kwargs"].get("json")
    assert payload["user_id"] == "alice"
    assert payload["group_id"] == "engineering"


def test_schedule_task(tmp_path):
    storage = tmp_path / "sched.yml"
    sched = CronScheduler(timezone="UTC", storage_path=storage)
    task = DummyTask()
    sched.schedule_task(task, "*/2 * * * *")
    job = sched.scheduler.get_job("DummyTask")
    assert job is not None
    data = yaml.safe_load(storage.read_text())
    assert data["DummyTask"] == "*/2 * * * *"


def test_schedule_task_user_id(tmp_path):
    storage = tmp_path / "sched.yml"
    sched = CronScheduler(timezone="UTC", storage_path=storage)
    task = DummyTask()
    sched.schedule_task(task, "*/2 * * * *", user_id="bob")
    data = yaml.safe_load(storage.read_text())
    from task_cascadence.ume import _hash_user_id

    assert data["DummyTask"]["expr"] == "*/2 * * * *"
    assert data["DummyTask"]["user_hash"] == _hash_user_id("bob")


def test_base_scheduler_has_no_schedule_task():
    bs = BaseScheduler()
    assert not hasattr(bs, "schedule_task")


def test_metrics_increment_for_job(tmp_path, monkeypatch):
    from task_cascadence import metrics

    storage = tmp_path / "sched.yml"
    sched = CronScheduler(timezone="UTC", storage_path=storage)
    task = DummyTask()

    # Prevent actual event emission
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda run, user_id=None: None)

    sched.register_task(name_or_task=task, task_or_expr="*/1 * * * *", user_id="alice")
    job = sched.scheduler.get_job("DummyTask")

    success = metrics.TASK_SUCCESS.labels("DummyTask")
    failure = metrics.TASK_FAILURE.labels("DummyTask")

    before_success = success._value.get()
    before_failure = failure._value.get()

    job.func()

    assert success._value.get() == before_success + 1
    assert failure._value.get() == before_failure


def test_run_task_user_id(monkeypatch):
    emitted = None

    from task_cascadence.ume import _hash_user_id

    def fake_emit(run, user_id=None):
        nonlocal emitted
        if user_id is not None:
            run.user_hash = _hash_user_id(user_id)
        emitted = run

    monkeypatch.setattr("task_cascadence.ume.emit_task_run", fake_emit)

    sched = BaseScheduler()

    class SimpleTask(CronTask):
        def run(self):
            return "ok"

    task = SimpleTask()
    sched.register_task("simple", task)
    sched.run_task("simple", user_id="bob")

    assert emitted is not None
    assert emitted.user_hash is not None
    assert emitted.user_hash != "bob"


def test_wrap_task_user_id(monkeypatch):
    emitted = None

    from task_cascadence.ume import _hash_user_id

    def fake_emit(run, user_id=None):
        nonlocal emitted
        if user_id is not None:
            run.user_hash = _hash_user_id(user_id)
        emitted = run

    monkeypatch.setattr("task_cascadence.ume.emit_task_run", fake_emit)

    sched = CronScheduler(timezone="UTC", storage_path="dummy.yml")
    task = DummyTask()
    sched.register_task("DummyTask", task)
    wrapped = sched._wrap_task(task, user_id="alice")
    wrapped()

    assert emitted is not None
    assert emitted.user_hash is not None
    assert emitted.user_hash != "alice"


def test_run_task_metrics_success(monkeypatch):
    from task_cascadence import metrics

    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda run, user_id=None: None)

    sched = BaseScheduler()

    class SimpleTask(CronTask):
        def run(self):
            return "ok"

    task = SimpleTask()
    sched.register_task("simple", task)

    success = metrics.TASK_SUCCESS.labels("SimpleTask")
    failure = metrics.TASK_FAILURE.labels("SimpleTask")

    before_success = success._value.get()
    before_failure = failure._value.get()

    result = sched.run_task("simple", user_id="alice")

    assert result == "ok"
    assert success._value.get() == before_success + 1
    assert failure._value.get() == before_failure


def test_run_task_metrics_failure(monkeypatch):
    from task_cascadence import metrics

    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda run, user_id=None: None)

    sched = BaseScheduler()

    class BoomTask(CronTask):
        def run(self):
            raise RuntimeError("boom")

    task = BoomTask()
    sched.register_task("boom", task)

    success = metrics.TASK_SUCCESS.labels("BoomTask")
    failure = metrics.TASK_FAILURE.labels("BoomTask")

    before_success = success._value.get()
    before_failure = failure._value.get()

    with pytest.raises(RuntimeError):
        sched.run_task("boom", user_id="alice")

    assert success._value.get() == before_success
    assert failure._value.get() == before_failure + 1


def test_register_task_twice_running(tmp_path):
    storage = tmp_path / "sched.yml"
    sched = CronScheduler(timezone="UTC", storage_path=storage)
    task = DummyTask()
    sched.start()
    try:
        sched.register_task(name_or_task=task, task_or_expr="* * * * *")
        sched.register_task(name_or_task=task, task_or_expr="*/2 * * * *")
        job = sched.scheduler.get_job("DummyTask")
    finally:
        sched.shutdown(wait=False)

    assert job is not None
    assert str(job.trigger) == "cron[month='*', day='*', day_of_week='*', hour='*', minute='*/2']"

    data = yaml.safe_load(storage.read_text())
    assert data["DummyTask"] == "*/2 * * * *"


def test_unschedule_removes_job(tmp_path, monkeypatch):
    storage = tmp_path / "sched.yml"
    sched = CronScheduler(timezone="UTC", storage_path=storage)
    task = DummyTask()
    sched.register_task(name_or_task=task, task_or_expr="*/5 * * * *")

    assert sched.scheduler.get_job("DummyTask") is not None

    path = tmp_path / "stages.yml"
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(path))
    import task_cascadence.ume as ume
    ume._stage_store = None

    sched.unschedule("DummyTask")

    assert sched.scheduler.get_job("DummyTask") is None
    data = yaml.safe_load(storage.read_text()) or {}
    assert "DummyTask" not in data
    events = yaml.safe_load(path.read_text())
    assert events["DummyTask"][-1]["stage"] == "unschedule"


def test_unschedule_unknown_job(tmp_path):
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    with pytest.raises(ValueError):
        sched.unschedule("MissingTask")


def test_scheduled_job_runs_pipeline(monkeypatch, tmp_path):
    path = tmp_path / "stages.yml"
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(path))

    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_task_spec",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_task_run",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        "task_cascadence.ume.emit_task_run",
        lambda *a, **k: None,
    )
    import task_cascadence.ume as ume
    ume._stage_store = None

    class DemoTask(CronTask):
        def intake(self):
            pass

        def plan(self):
            return None

        def run(self):
            return "ok"

        def verify(self, result):
            return result

    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    task = DemoTask()
    sched.register_task(name_or_task=task, task_or_expr="* * * * *", user_id="alice")

    job = sched.scheduler.get_job("DemoTask")
    assert job is not None
    job.func()

    data = yaml.safe_load(path.read_text())
    stages = [e["stage"] for e in data["DemoTask"]]
    assert stages == ["intake", "research", "planning", "run", "verification"]


class DummyResp:
    def __init__(self, data: dict[str, str]) -> None:
        self._data = data

    def raise_for_status(self) -> None:  # pragma: no cover - compat
        pass

    def json(self) -> dict[str, str]:
        return self._data


def test_cronyx_run_task_hash(monkeypatch):
    captured = {}

    def fake_request(method, url, timeout=0, **kwargs):
        captured["json"] = kwargs.get("json")
        return DummyResp({"result": "ok"})

    monkeypatch.setattr(requests, "request", fake_request)
    from task_cascadence.scheduler.cronyx import CronyxScheduler
    from task_cascadence.ume import _hash_user_id

    sched = CronyxScheduler(base_url="http://server")
    sched.run_task("demo", user_id="alice")

    assert captured["json"]["user_id"] == _hash_user_id("alice")


def test_cronyx_schedule_task_hash(monkeypatch):
    captured = {}

    def fake_request(method, url, timeout=0, **kwargs):
        captured["json"] = kwargs.get("json")
        return DummyResp({"scheduled": True})

    monkeypatch.setattr(requests, "request", fake_request)
    from task_cascadence.scheduler.cronyx import CronyxScheduler
    from task_cascadence.ume import _hash_user_id

    sched = CronyxScheduler(base_url="http://server")
    sched.schedule_task("demo", "* * * * *", user_id="bob")

    assert captured["json"]["user_id"] == _hash_user_id("bob")

