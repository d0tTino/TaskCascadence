from typer.testing import CliRunner
import importlib
import logging

from task_cascadence.cli import app
from task_cascadence import capability_planner


class DummyScheduler:
    def __init__(self):
        self.tasks = []

    def register_task(self, name, task):
        self.tasks.append(name)


def test_capability_planner_grpc(monkeypatch, tmp_path):
    monkeypatch.setenv("UME_TRANSPORT", "grpc")
    monkeypatch.setenv("UME_GRPC_METHOD", "Subscribe")

    module = tmp_path / "stub_planner.py"
    module.write_text(
        """
class Stub:
    @staticmethod
    def Subscribe():
        from task_cascadence.ume.protos.tasks_pb2 import TaskRun, TaskSpec
        return [TaskRun(spec=TaskSpec(id='demo', name='demo'), run_id='r1', status='error')]
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("UME_GRPC_STUB", "stub_planner:Stub")

    importlib.invalidate_caches()
    sched = DummyScheduler()
    monkeypatch.setattr(capability_planner, "get_default_scheduler", lambda: sched)

    capability_planner.run()

    assert sched.tasks == ["capability_follow_up"]


def test_capability_planner_nats(monkeypatch, tmp_path):
    monkeypatch.setenv("UME_TRANSPORT", "nats")

    module = tmp_path / "conn_planner.py"
    module.write_text(
        """
class Conn:
    def subscribe_sync(self, subject):
        from task_cascadence.ume.protos.tasks_pb2 import TaskRun, TaskSpec
        run = TaskRun(spec=TaskSpec(id='demo', name='demo'), run_id='r2', status='error')
        return [run.SerializeToString()]
conn = Conn()
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("UME_NATS_CONN", "conn_planner:conn")
    monkeypatch.setenv("UME_NATS_SUBJECT", "events")

    importlib.invalidate_caches()
    sched = DummyScheduler()
    monkeypatch.setattr(capability_planner, "get_default_scheduler", lambda: sched)

    capability_planner.run()

    assert sched.tasks == ["capability_follow_up"]


def test_cli_capability_planner(monkeypatch, tmp_path):
    monkeypatch.setenv("UME_TRANSPORT", "grpc")
    monkeypatch.setenv("UME_GRPC_METHOD", "Subscribe")

    module = tmp_path / "stub_cli_planner.py"
    module.write_text(
        """
class Stub:
    @staticmethod
    def Subscribe():
        from task_cascadence.ume.protos.tasks_pb2 import TaskRun, TaskSpec
        return [TaskRun(spec=TaskSpec(id='demo', name='demo'), run_id='cli', status='error')]
"""
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.setenv("UME_GRPC_STUB", "stub_cli_planner:Stub")

    importlib.invalidate_caches()
    sched = DummyScheduler()
    monkeypatch.setattr(capability_planner, "get_default_scheduler", lambda: sched)

    runner = CliRunner()
    result = runner.invoke(app, ["capability-planner"])
    assert result.exit_code == 0
    assert sched.tasks == ["capability_follow_up"]


def test_capability_planner_no_transport(monkeypatch, caplog):
    sched = DummyScheduler()
    monkeypatch.setattr(capability_planner, "get_default_scheduler", lambda: sched)
    monkeypatch.delenv("UME_TRANSPORT", raising=False)

    with caplog.at_level(logging.WARNING):
        capability_planner.run()

    assert sched.tasks == []
    assert any(
        "UME transport not configured. Exiting." in r.getMessage()
        for r in caplog.records
    )


def test_capability_planner_unknown_transport(monkeypatch, caplog):
    sched = DummyScheduler()
    monkeypatch.setattr(capability_planner, "get_default_scheduler", lambda: sched)
    monkeypatch.setenv("UME_TRANSPORT", "foo")

    with caplog.at_level(logging.WARNING):
        capability_planner.run()

    assert sched.tasks == []
    assert any(
        "Unknown UME transport: foo" in r.getMessage()
        for r in caplog.records
    )
