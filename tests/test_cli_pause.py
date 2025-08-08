from typer.testing import CliRunner

from task_cascadence.cli import app
from task_cascadence.scheduler import CronScheduler
from task_cascadence.plugins import ExampleTask, CronTask
from task_cascadence.stage_store import StageStore
from task_cascadence.pipeline_registry import get_pipeline
from task_cascadence.orchestrator import TaskPipeline
import asyncio
import inspect
import threading
import time



def test_cli_pause_resume(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "stages.yml"))
    import task_cascadence.ume as ume
    ume._stage_store = None

    sched = CronScheduler(storage_path=tmp_path / "sched.yml")
    task = ExampleTask()
    sched.register_task("example", task)
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)

    runner = CliRunner()
    result = runner.invoke(app, ["pause", "example"])
    assert result.exit_code == 0
    assert sched._tasks["example"]["paused"] is True
    events = StageStore(path=tmp_path / "stages.yml").get_events("example")
    assert events[-1]["stage"] == "paused"

    result = runner.invoke(app, ["resume", "example"])
    assert result.exit_code == 0
    assert sched._tasks["example"]["paused"] is False
    events = StageStore(path=tmp_path / "stages.yml").get_events("example")
    assert events[-1]["stage"] == "resumed"


def test_cli_pause(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "stages.yml"))
    import task_cascadence.ume as ume
    ume._stage_store = None

    sched = CronScheduler(storage_path=tmp_path / "sched_pause.yml")
    task = ExampleTask()
    sched.register_task("example", task)
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)

    runner = CliRunner()
    result = runner.invoke(app, ["pause", "example"])

    assert result.exit_code == 0
    assert sched._tasks["example"]["paused"] is True
    events = StageStore(path=tmp_path / "stages.yml").get_events("example")
    assert events[-1]["stage"] == "paused"


def test_cli_resume(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "stages.yml"))
    import task_cascadence.ume as ume
    ume._stage_store = None

    sched = CronScheduler(storage_path=tmp_path / "sched_resume.yml")
    task = ExampleTask()
    sched.register_task("example", task)
    sched.pause_task("example")
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)

    runner = CliRunner()
    result = runner.invoke(app, ["resume", "example"])

    assert result.exit_code == 0
    assert sched._tasks["example"]["paused"] is False
    events = StageStore(path=tmp_path / "stages.yml").get_events("example")
    assert events[-1]["stage"] == "resumed"


class SlowTask(CronTask):
    name = "slow"

    def intake(self):
        time.sleep(0.05)

    def run(self):
        time.sleep(0.2)
        return "ok"


def test_pause_running_pipeline(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "stages.yml"))
    import task_cascadence.ume as ume
    ume._stage_store = None

    sched = CronScheduler(storage_path=tmp_path / "sched2.yml")
    task = SlowTask()
    sched.register_task("slow", task)
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda *a, **k: None)

    runner = CliRunner()
    thread = threading.Thread(target=lambda: sched.run_task("slow", user_id="bob"))
    thread.start()
    time.sleep(0.05)
    result = runner.invoke(app, ["pause", "slow", "--user-id", "bob"])
    assert result.exit_code == 0
    pipeline = get_pipeline("slow")
    assert pipeline is not None and pipeline._paused is True
    assert thread.is_alive()
    result = runner.invoke(app, ["resume", "slow", "--user-id", "bob"])
    assert result.exit_code == 0
    thread.join()
    assert pipeline._paused is False


class DummyTask(CronTask):
    def __init__(self):
        self.count = 0

    def run(self):
        self.count += 1


def test_paused_job_does_not_run(monkeypatch, tmp_path):
    sched = CronScheduler(timezone="UTC", storage_path=tmp_path / "s.yml")
    task = DummyTask()
    sched.register_task(name_or_task=task, task_or_expr="* * * * *")
    sched.pause_task("DummyTask")

    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda run, user_id=None: None)

    job = sched.scheduler.get_job("DummyTask")
    job.func()

    assert task.count == 0


def test_pipeline_pause_non_blocking(monkeypatch):
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda *a, **k: None)

    class SimpleTask(CronTask):
        name = "simple"

        def run(self):
            return "ok"

    pipeline = TaskPipeline(SimpleTask())
    pipeline.pause(user_id="alice")

    ran = {"flag": False}

    async def other() -> None:
        await asyncio.sleep(0.01)
        ran["flag"] = True

    async def resume_later() -> None:
        await asyncio.sleep(0.05)
        pipeline.resume(user_id="alice")

    async def runner() -> str:
        result = pipeline.run(user_id="alice")
        if inspect.isawaitable(result):
            result = await result
        return result

    async def main() -> None:
        result = await asyncio.gather(runner(), other(), resume_later())
        assert result[0] == "ok"
        assert ran["flag"] is True

    asyncio.run(main())

