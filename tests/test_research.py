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
