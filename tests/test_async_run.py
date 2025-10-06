import asyncio
from task_cascadence.scheduler import BaseScheduler


class AsyncTask:
    async def run(self):
        await asyncio.sleep(0)
        return "async"

def test_async_run(monkeypatch):
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda *a, **k: None)

    sched = BaseScheduler()
    sched.register_task("async", AsyncTask())
    execution = sched.run_task_with_metadata("async", user_id="alice")
    assert execution.result == "async"
    assert execution.run_id
