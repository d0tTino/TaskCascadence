import yaml
import pytest
from apscheduler.triggers.cron import CronTrigger
from task_cascadence.scheduler import CronScheduler
from task_cascadence.ume import _hash_user_id


def expected_hash(value: str) -> str:
    return _hash_user_id(value)


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
    sched.start()
    task = DummyTask()
    sched.load_yaml(cfg, {"DummyTask": task})
    job = sched.scheduler.get_job("DummyTask")
    assert job is not None
    assert sched.schedules["DummyTask"]["recurrence"] == {"note": "every minute"}


def test_load_yaml_malformed(tmp_path):
    cfg = tmp_path / "bad.yml"
    cfg.write_text("DummyTask: [1, 2")
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    with pytest.raises(yaml.YAMLError):
        sched.load_yaml(cfg, {"DummyTask": DummyTask()})


def test_load_yaml_missing_required_fields(tmp_path):
    data = {"DummyTask": {}}
    cfg = tmp_path / "config.yml"
    cfg.write_text(yaml.safe_dump(data))
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    with pytest.raises(
        ValueError, match="DummyTask: missing 'expr' or 'calendar_event'"
    ):
        sched.load_yaml(cfg, {"DummyTask": DummyTask()})


def test_load_yaml_calendar_event_missing_node(tmp_path):
    data = {"DummyTask": {"calendar_event": {}}}
    cfg = tmp_path / "config.yml"
    cfg.write_text(yaml.safe_dump(data))
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    with pytest.raises(
        ValueError, match="calendar_event missing 'node'"
    ):
        sched.load_yaml(cfg, {"DummyTask": DummyTask()})


def test_load_yaml_calendar_event_invalid_type(tmp_path):
    data = {"DummyTask": {"calendar_event": 123}}
    cfg = tmp_path / "config.yml"
    cfg.write_text(yaml.safe_dump(data))
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    with pytest.raises(
        ValueError, match="'calendar_event' must be string or mapping"
    ):
        sched.load_yaml(cfg, {"DummyTask": DummyTask()})


def test_load_yaml_expr_invalid_type(tmp_path):
    data = {"DummyTask": {"expr": 123}}
    cfg = tmp_path / "config.yml"
    cfg.write_text(yaml.safe_dump(data))
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    with pytest.raises(
        ValueError, match="'expr' must be a non-empty string"
    ):
        sched.load_yaml(cfg, {"DummyTask": DummyTask()})


def test_schedule_from_calendar_event(tmp_path):
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    sched.start()
    task = DummyTask()
    event = {"title": "review", "recurrence": {"cron": "*/2 * * * *"}}
    sched.schedule_from_event(
        task, event, user_id="alice", group_id="engineering"
    )
    job = sched.scheduler.get_job("DummyTask")
    assert job is not None
    entry = sched.schedules["DummyTask"]
    assert entry["recurrence"] == {"cron": "*/2 * * * *"}
    expected_user = expected_hash("alice")
    expected_group = expected_hash("engineering")
    assert entry["user_hash"] == expected_user
    assert entry["group_hash"] == expected_group
    persisted = yaml.safe_load((tmp_path / "sched.yml").read_text())
    assert persisted["DummyTask"]["user_hash"] == expected_user
    assert persisted["DummyTask"]["group_hash"] == expected_group
    assert persisted["DummyTask"]["recurrence"] == {"cron": "*/2 * * * *"}


def test_loads_plain_identifiers_and_rewrites(tmp_path):
    sched_file = tmp_path / "sched.yml"
    data = {
        "DummyTask": {
            "expr": "* * * * *",
            "user_id": "alice",
            "group_id": "engineering",
        }
    }
    sched_file.write_text(yaml.safe_dump(data))
    sched = CronScheduler(timezone="UTC", storage_path=sched_file)
    expected_user = expected_hash("alice")
    expected_group = expected_hash("engineering")
    assert sched.schedules["DummyTask"]["user_hash"] == expected_user
    assert sched.schedules["DummyTask"]["group_hash"] == expected_group
    persisted = yaml.safe_load(sched_file.read_text())
    assert persisted["DummyTask"]["user_hash"] == expected_user
    assert persisted["DummyTask"]["group_hash"] == expected_group


def test_yaml_calendar_event_daily(tmp_path, monkeypatch):
    data = {"DummyTask": {"calendar_event": "evt1"}}
    cfg = tmp_path / "config.yml"
    cfg.write_text(yaml.safe_dump(data))
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    sched.start()
    task = DummyTask()

    def fake_fetch(self, node, *, user_id=None, group_id=None):
        assert node == "evt1"
        assert user_id is None and group_id is None
        return {"recurrence": {"cron": "0 9 * * *"}}

    monkeypatch.setattr(CronScheduler, "_fetch_calendar_event", fake_fetch)
    sched.load_yaml(cfg, {"DummyTask": task})
    job = sched.scheduler.get_job("DummyTask")
    assert job is not None
    expected = CronTrigger.from_crontab("0 9 * * *", timezone="UTC")
    assert str(job.trigger) == str(expected)
    entry = sched.schedules["DummyTask"]
    assert entry["calendar_event"] == {"node": "evt1"}


def test_yaml_calendar_event_weekly(tmp_path, monkeypatch):
    data = {"DummyTask": {"calendar_event": "evt2"}}
    cfg = tmp_path / "config.yml"
    cfg.write_text(yaml.safe_dump(data))
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    sched.start()
    task = DummyTask()

    def fake_fetch(self, node, *, user_id=None, group_id=None):
        assert node == "evt2"
        assert user_id is None and group_id is None
        return {"recurrence": {"cron": "30 10 * * 1"}}

    monkeypatch.setattr(CronScheduler, "_fetch_calendar_event", fake_fetch)
    sched.load_yaml(cfg, {"DummyTask": task})
    job = sched.scheduler.get_job("DummyTask")
    assert job is not None
    expected = CronTrigger.from_crontab("30 10 * * 1", timezone="UTC")
    assert str(job.trigger) == str(expected)
    entry = sched.schedules["DummyTask"]
    assert entry["calendar_event"] == {"node": "evt2"}


def test_yaml_calendar_event_recurrence_change(tmp_path, monkeypatch):
    data = {"DummyTask": {"calendar_event": {"node": "evt4", "poll": 1}}}
    cfg = tmp_path / "config.yml"
    cfg.write_text(yaml.safe_dump(data))
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    sched.start()
    task = DummyTask()

    events = [
        {"recurrence": {"cron": "0 9 * * *"}},
        {"recurrence": {"cron": "0 10 * * *"}},
    ]

    def fake_fetch(self, node, *, user_id=None, group_id=None):
        return events.pop(0)

    monkeypatch.setattr(CronScheduler, "_fetch_calendar_event", fake_fetch)
    sched.load_yaml(cfg, {"DummyTask": task})
    job = sched.scheduler.get_job("DummyTask")
    assert job is not None
    expected = CronTrigger.from_crontab("0 9 * * *", timezone="UTC")
    assert str(job.trigger) == str(expected)

    poll_job = sched.scheduler.get_job("poll:DummyTask:evt4")
    assert poll_job is not None
    poll_job.func()

    job = sched.scheduler.get_job("DummyTask")
    expected = CronTrigger.from_crontab("0 10 * * *", timezone="UTC")
    assert str(job.trigger) == str(expected)
    entry = sched.schedules["DummyTask"]
    assert entry["recurrence"] == {"cron": "0 10 * * *"}
    sched.shutdown()


def test_yaml_calendar_event_multiple_recurrences(tmp_path, monkeypatch):
    data = {
        "DummyTask": {
            "calendar_event": "evt3",
            "user_id": "alice",
            "group_id": "engineering",
        }
    }
    cfg = tmp_path / "config.yml"
    cfg.write_text(yaml.safe_dump(data))
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    task = DummyTask()

    def fake_fetch(self, node, *, user_id=None, group_id=None):
        assert node == "evt3"
        assert user_id == "alice"
        assert group_id == "engineering"
        return {"recurrence": {"cron": "*/2 * * * *"}}

    monkeypatch.setattr(CronScheduler, "_fetch_calendar_event", fake_fetch)

    captured: list[tuple[str | None, str | None, str | None]] = []

    import sys
    import types

    ume_mod = types.ModuleType("task_cascadence.ume")
    models_mod = types.ModuleType("task_cascadence.ume.models")

    class TaskSpec:
        def __init__(self, id: str, name: str):
            self.id = id
            self.name = name

    class TaskRun:
        def __init__(self, spec, run_id, status, started_at, finished_at):
            self.spec = spec
            self.run_id = run_id
            self.status = status
            self.started_at = started_at
            self.finished_at = finished_at
            self.user_hash = None

    models_mod.TaskSpec = TaskSpec
    models_mod.TaskRun = TaskRun
    monkeypatch.setitem(sys.modules, "task_cascadence.ume.models", models_mod)
    ume_mod.models = models_mod

    orch_mod = types.ModuleType("task_cascadence.orchestrator")

    class TaskPipeline:
        def __init__(self, task):
            self._task = task

        def run(self, user_id=None, group_id=None):  # pragma: no cover - simple stub
            return self._task.run()

    orch_mod.TaskPipeline = TaskPipeline
    monkeypatch.setitem(sys.modules, "task_cascadence.orchestrator", orch_mod)

    pr_mod = types.ModuleType("task_cascadence.pipeline_registry")

    def add_pipeline(name, pipeline):  # pragma: no cover - stub
        pass

    def remove_pipeline(name):  # pragma: no cover - stub
        pass

    pr_mod.add_pipeline = add_pipeline
    pr_mod.remove_pipeline = remove_pipeline
    monkeypatch.setitem(sys.modules, "task_cascadence.pipeline_registry", pr_mod)

    def fake_emit(run, user_id=None, group_id=None):
        captured.append((run.user_hash, user_id, group_id))

    ume_mod.emit_task_run = fake_emit
    monkeypatch.setitem(sys.modules, "task_cascadence.ume", ume_mod)

    sched.load_yaml(cfg, {"DummyTask": task})
    job = sched.scheduler.get_job("DummyTask")
    assert job is not None

    import datetime as dt

    start = dt.datetime(2024, 1, 1, 9, 0, tzinfo=job.trigger.timezone)
    first = job.trigger.get_next_fire_time(None, start)
    second = job.trigger.get_next_fire_time(first, first)
    assert (second - first) == dt.timedelta(minutes=2)

    times = iter(
        [
            first,
            first + dt.timedelta(seconds=1),
            second,
            second + dt.timedelta(seconds=1),
        ]
    )

    class FrozenDatetime(dt.datetime):
        @classmethod
        def now(cls, tz=None):  # pragma: no cover - simple wrapper
            return next(times)

    monkeypatch.setattr(dt, "datetime", FrozenDatetime)

    job.func()
    job.func()

    assert task.count == 2
    expected_group = expected_hash("engineering")
    assert captured == [
        (captured[0][0], "alice", expected_group),
        (captured[1][0], "alice", expected_group),
    ]
