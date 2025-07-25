from task_cascadence.orchestrator import TaskPipeline
from task_cascadence.ume import _hash_user_id
from task_cascadence.scheduler import BaseScheduler


class DemoTask:
    def __init__(self, steps):
        self.steps = steps

    def intake(self):
        self.steps.append("intake")

    def plan(self):
        self.steps.append("plan")
        return "plan"

    def run(self):
        self.steps.append("run")
        return "result"

    def verify(self, result):
        self.steps.append(f"verify:{result}")
        return "ok"


def test_pipeline_emits_events(monkeypatch):
    steps: list[str] = []
    emitted = []

    def fake_spec(spec, user_id=None):
        if user_id is not None:
            spec.user_hash = _hash_user_id(user_id)
        emitted.append(("spec", spec.description, spec.user_hash))

    def fake_run(run, user_id=None):
        if user_id is not None:
            run.user_hash = _hash_user_id(user_id)
        emitted.append(("run", run.status, run.user_hash))

    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", fake_spec)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", fake_run)

    task = DemoTask(steps)
    pipeline = TaskPipeline(task)

    result = pipeline.run(user_id="bob")

    assert result == "ok"
    assert steps == ["intake", "plan", "run", "verify:result"]
    stages = [e[0] for e in emitted]
    assert stages == ["spec", "spec", "run", "spec"]
    assert emitted[0][2] == _hash_user_id("bob")
    assert emitted[2][2] == _hash_user_id("bob")


def test_pipeline_without_optional(monkeypatch):
    emitted = []

    def fake_spec(spec, user_id=None):
        emitted.append(spec.description)

    def fake_run(run, user_id=None):
        emitted.append("run")

    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", fake_spec)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", fake_run)

    class Simple:
        def run(self):
            return "done"

    pipeline = TaskPipeline(Simple())
    result = pipeline.run()

    assert result == "done"
    assert emitted == ["intake", "planning", "run", "verification"]


def test_scheduler_runs_pipeline(monkeypatch):
    steps: list[str] = []

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

    class Demo:
        def __init__(self, steps):
            self.steps = steps

        def intake(self):
            self.steps.append("intake")

        def run(self):
            self.steps.append("run")
            return "ok"

    sched = BaseScheduler()
    sched.register_task("demo", Demo(steps))
    result = sched.run_task("demo")

    assert result == "ok"
    assert steps == ["intake", "run"]


def test_plan_result_passed_to_run(monkeypatch):
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)

    class PlanTask:
        def __init__(self):
            self.received = None

        def plan(self):
            return "myplan"

        def run(self, plan):
            self.received = plan
            return plan

    task = PlanTask()
    pipeline = TaskPipeline(task)
    result = pipeline.run()

    assert result == "myplan"
    assert task.received == "myplan"
