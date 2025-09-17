import pytest

from task_cascadence.scheduler import CronScheduler
from task_cascadence.ume import _hash_user_id


def test_scheduler_job_exception_emits_audit_log(monkeypatch, tmp_path):
    calls: list[tuple[str, str, str, dict]] = []

    def fake_emit(task_name, stage, status, client=None, **kwargs):
        if stage == "run":
            calls.append((task_name, stage, status, kwargs))

    monkeypatch.setattr("task_cascadence.ume.emit_audit_log", fake_emit)

    class ExplodingTask:
        def run(self):
            raise RuntimeError("boom")

    scheduler = CronScheduler(timezone="UTC", storage_path=tmp_path / "sched.yml")
    task = ExplodingTask()
    scheduler.register_task(
        name_or_task=task,
        task_or_expr="*/5 * * * *",
        user_id="alice",
        group_id="team",
    )

    job = scheduler.scheduler.get_job("ExplodingTask")
    assert job is not None

    with pytest.raises(RuntimeError, match="boom"):
        job.func()

    assert len(calls) == 1
    task_name, stage, status, kwargs = calls[0]
    assert task_name == "ExplodingTask"
    assert stage == "run"
    assert status == "error"
    assert kwargs["reason"] == "boom"
    assert kwargs["user_id"] == _hash_user_id("alice")
    assert kwargs["group_id"] == _hash_user_id("team")
    if "output" in kwargs:
        assert kwargs["output"] is None
    if "partial" in kwargs:
        assert kwargs["partial"] is None
