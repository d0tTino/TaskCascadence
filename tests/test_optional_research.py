from task_cascadence.orchestrator import TaskPipeline


def test_pipeline_runs_without_tino_storm(monkeypatch):
    """Pipelines should run even when ``tino_storm`` is missing."""
    emitted = []

    def fake_spec(spec, user_id=None):
        emitted.append(spec.description)

    def fake_run(run, user_id=None):
        emitted.append("run")

    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", fake_spec)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", fake_run)

    import task_cascadence.research as research

    monkeypatch.setattr(research, "tino_storm", None)

    class ResearchTask:
        def research(self):
            return "foo"

        def plan(self):
            return "plan"

        def run(self, plan):
            return "result"

        def verify(self, result):
            return "ok"

    pipeline = TaskPipeline(ResearchTask())
    result = pipeline.run(user_id="u1")

    assert result == "ok"
    assert emitted == ["intake", "research", "planning", "run", "verification"]
