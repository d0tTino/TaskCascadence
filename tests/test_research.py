import asyncio

from task_cascadence.orchestrator import TaskPipeline


def test_pipeline_research(monkeypatch):
    steps = []
    emitted = []

    def fake_spec(spec, user_id=None):
        emitted.append(spec.description)

    def fake_run(run, user_id=None):
        emitted.append("run")

    def fake_gather(query: str):
        steps.append(f"research:{query}")
        return "info"

    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", fake_spec)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", fake_run)
    monkeypatch.setattr("task_cascadence.research.gather", fake_gather)

    class ResearchTask:
        def research(self):
            return "foo"

        def plan(self):
            steps.append("plan")
            return "plan"

        def run(self):
            steps.append("run")
            return "result"

        def verify(self, result):
            steps.append(f"verify:{result}")
            return "ok"

    pipeline = TaskPipeline(ResearchTask())
    result = pipeline.run()

    assert result == "ok"
    assert steps == ["research:foo", "plan", "run", "verify:result"]
    assert emitted == ["intake", "research", "planning", "run", "verification"]


def test_async_pipeline_research(monkeypatch):
    steps = []
    emitted = []

    def fake_spec(spec, user_id=None):
        emitted.append(spec.description)

    async def fake_async_gather(query: str):
        steps.append(f"research:{query}")
        return "ainfo"

    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", fake_spec)
    monkeypatch.setattr("task_cascadence.research.async_gather", fake_async_gather)

    class ResearchTask:
        def research(self):
            return "foo"

    pipeline = TaskPipeline(ResearchTask())

    async def runner():
        result = await pipeline.research()
        return result

    result = asyncio.run(runner())

    assert result == "ainfo"
    assert steps == ["research:foo"]
    assert emitted == ["research"]


async def async_research_method():
    await asyncio.sleep(0)
    return "bar"


def test_async_research_method(monkeypatch):
    steps = []
    emitted = []

    def fake_spec(spec, user_id=None):
        emitted.append(spec.description)

    async def fake_async_gather(query: str):
        steps.append(f"research:{query}")
        return "info2"

    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", fake_spec)
    monkeypatch.setattr("task_cascadence.research.async_gather", fake_async_gather)

    class ResearchTask:
        async def research(self):
            return async_research_method()

    pipeline = TaskPipeline(ResearchTask())

    async def runner():
        return await pipeline.research()

    result = asyncio.run(runner())

    assert result == "info2"
    assert steps == ["research:bar"]
    assert emitted == ["research"]
