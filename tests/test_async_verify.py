import asyncio
from task_cascadence.orchestrator import TaskPipeline


class DemoTask:
    def run(self):
        return "ok"

    async def verify(self, result):
        await asyncio.sleep(0)
        return f"verified-{result}"


def test_async_verify(monkeypatch):
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)
    pipeline = TaskPipeline(DemoTask())
    result = pipeline.run()
    assert result == "verified-ok"
