import asyncio
import inspect
import pytest
from task_cascadence.orchestrator import TaskPipeline, ParallelPlan
from task_cascadence.ume import _hash_user_id
from task_cascadence.scheduler import BaseScheduler
from task_cascadence.stage_store import StageStore


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
    assert stages == ["spec", "spec", "spec", "run", "spec"]
    assert emitted[0][2] == _hash_user_id("bob")
    assert emitted[3][2] == _hash_user_id("bob")


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
    result = pipeline.run(user_id="bob")

    assert result == "done"
    assert emitted == ["intake", "research", "plan", "run", "verify"]


def test_pipeline_group_id(monkeypatch):
    emitted = []

    def fake_spec(spec, user_id=None, group_id=None):
        emitted.append(("spec", group_id))

    def fake_run(run, user_id=None, group_id=None):
        emitted.append(("run", group_id))

    def fake_stage(
        task_name,
        stage,
        client=None,
        user_id=None,
        group_id=None,
        use_asyncio=False,
        run_id=None,
        **_kwargs,
    ):
        emitted.append((stage, group_id, run_id))

    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", fake_spec)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", fake_run)
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_stage_update_event", fake_stage
    )

    class Simple:
        def run(self):
            return "ok"

    pipeline = TaskPipeline(Simple())
    pipeline.run(user_id="alice", group_id="team1")

    gids = [entry[1] for entry in emitted]
    assert gids and all(g == "team1" for g in gids)


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
    result = sched.run_task("demo", user_id="alice")

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
    result = pipeline.run(user_id="alice")

    assert result == "myplan"
    assert task.received == "myplan"


def test_nested_task_execution(monkeypatch):
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)

    steps: list[str] = []

    class Child:
        def __init__(self, name: str):
            self.name = name

        def run(self) -> str:
            steps.append(f"run-{self.name}")
            return self.name

    class Parent:
        def plan(self):
            return [Child("a"), TaskPipeline(Child("b"))]

        def verify(self, result):
            steps.append(f"verify-{result}")
            return result

    pipeline = TaskPipeline(Parent())
    result = pipeline.run(user_id="alice")

    assert result == ["a", "b"]
    assert steps == ["run-a", "run-b", "verify-['a', 'b']"]


def test_nested_tasks_parent_run(monkeypatch):
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)

    steps: list[str] = []

    class Child:
        def __init__(self, name: str):
            self.name = name

        def run(self) -> str:
            steps.append(f"run-{self.name}")
            return self.name

    class Parent:
        def plan(self):
            return [Child("x"), Child("y")]

        def run(self, results):
            steps.append(f"parent-run-{results}")
            return results

    pipeline = TaskPipeline(Parent())
    result = pipeline.run(user_id="alice")

    assert result == ["x", "y"]
    assert steps == ["run-x", "run-y", "parent-run-['x', 'y']"]


def test_subtask_results_resolved_sync(monkeypatch):
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)

    class SyncChild:
        def run(self) -> str:
            return "sync"

    class AsyncChild:
        async def run(self) -> str:
            await asyncio.sleep(0)
            return "async"

    class Parent:
        def __init__(self) -> None:
            self.received: list[str] | None = None

        def plan(self):
            return [SyncChild(), AsyncChild()]

        def run(self, results: list[str]):
            self.received = results
            return results

    parent = Parent()
    pipeline = TaskPipeline(parent)
    result = pipeline.run(user_id="alice")

    assert result == ["sync", "async"]
    assert parent.received == ["sync", "async"]
    assert all(not inspect.isawaitable(r) for r in result)


@pytest.mark.asyncio
async def test_subtask_results_resolved_async(monkeypatch):
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)

    class SyncChild:
        def run(self) -> str:
            return "sync"

    class AsyncChild:
        async def run(self) -> str:
            await asyncio.sleep(0)
            return "async"

    class Parent:
        def __init__(self) -> None:
            self.received: list[str] | None = None

        def plan(self):
            return [SyncChild(), AsyncChild()]

        def run(self, results: list[str]):
            self.received = results
            return results

    parent = Parent()
    pipeline = TaskPipeline(parent)
    result = await pipeline.run(user_id="alice")

    assert result == ["sync", "async"]
    assert parent.received == ["sync", "async"]
    assert all(not inspect.isawaitable(r) for r in result)


def test_precheck_failure_stops_pipeline(monkeypatch):
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)

    steps: list[str] = []

    class PrecheckTask:
        def precheck(self) -> bool:
            steps.append("precheck")
            return False

        def run(self) -> str:
            steps.append("run")
            return "ok"

    pipeline = TaskPipeline(PrecheckTask())

    with pytest.raises(RuntimeError):
        pipeline.run(user_id="alice")

    assert steps == ["precheck"]


async def _delay(value: str) -> str:
    await asyncio.sleep(0.05)
    return value


def test_parallel_execution_dict(monkeypatch):
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)

    class Child:
        def __init__(self, name: str):
            self.name = name

        async def run(self) -> str:
            return await _delay(self.name)

    class Parent:
        def plan(self):
            return {"execution": "parallel", "tasks": [Child("a"), Child("b")]} 

    pipeline = TaskPipeline(Parent())
    result = pipeline.run(user_id="alice")

    assert sorted(result) == ["a", "b"]


def test_parallel_execution_object(monkeypatch):
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)

    class Child:
        def __init__(self, name: str):
            self.name = name

        async def run(self) -> str:
            return await _delay(self.name)

    class Parent:
        def plan(self):
            return ParallelPlan([Child("x"), Child("y")])

    pipeline = TaskPipeline(Parent())
    result = pipeline.run(user_id="alice")

    assert sorted(result) == ["x", "y"]


def test_async_verify(monkeypatch, tmp_path):
    """An async ``verify`` method is awaited and its result returned."""

    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "stages.yml"))
    import task_cascadence.ume as ume
    ume._stage_store = None

    stages: list[str] = []

    def fake_spec(spec, user_id=None):
        stages.append(spec.description)

    def fake_run(run, user_id=None):
        stages.append("run")

    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", fake_spec)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", fake_run)

    class AsyncTask:
        async def run(self):
            return "done"

        async def verify(self, result):
            await asyncio.sleep(0.01)
            return f"verified:{result}"

    pipeline = TaskPipeline(AsyncTask())
    result = pipeline.run(user_id="alice")

    assert result == "verified:done"
    assert stages == ["intake", "research", "plan", "run", "verify"]

    events = StageStore(path=tmp_path / "stages.yml").get_events("AsyncTask")
    assert [e["stage"] for e in events] == ["intake", "research", "plan", "run", "verify"]


def test_pipeline_run_async(monkeypatch):
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None)

    class AsyncTask:
        async def run(self):
            await asyncio.sleep(0)
            return "ok"

    pipeline = TaskPipeline(AsyncTask())
    result = asyncio.run(pipeline.run_async(user_id="alice"))
    assert result == "ok"


def test_run_async_parallel_plan(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "stages.yml"))
    import task_cascadence.ume as ume
    ume._stage_store = None

    stages: list[str] = []

    def fake_spec(spec, user_id=None):
        if spec.name == "Parent":
            stages.append(spec.description)

    def fake_run(run, user_id=None):
        if run.spec.name == "Parent":
            stages.append("run")

    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", fake_spec)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", fake_run)

    class Child:
        def __init__(self, name: str):
            self.name = name

        async def run(self) -> str:
            await asyncio.sleep(0)
            return self.name

    class Parent:
        def plan(self):
            return ParallelPlan([Child("a"), Child("b")])

    pipeline = TaskPipeline(Parent())
    result = asyncio.run(pipeline.run_async(user_id="alice"))

    assert sorted(result) == ["a", "b"]
    assert stages == ["intake", "research", "plan", "run", "verify"]

    events = StageStore(path=tmp_path / "stages.yml").get_events("Parent")
    assert [e["stage"] for e in events] == ["intake", "research", "plan", "run", "verify"]


def test_run_list_plan_emits_run_stage(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "stages.yml"))
    import task_cascadence.ume as ume

    ume._stage_store = None

    stages: list[str] = []

    def fake_spec(spec, user_id=None):
        if spec.name == "Parent":
            stages.append(spec.description)

    def fake_run(run, user_id=None):
        if run.spec.name == "Parent":
            stages.append("run")

    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_spec", fake_spec)
    monkeypatch.setattr("task_cascadence.orchestrator.emit_task_run", fake_run)

    class Child:
        def __init__(self, name: str):
            self.name = name

        def run(self) -> str:
            return self.name

    class Parent:
        def plan(self):
            return [Child("a"), Child("b")]

    pipeline = TaskPipeline(Parent())
    result = pipeline.run(user_id="alice")

    assert result == ["a", "b"]
    assert stages == ["intake", "research", "plan", "run", "verify"]

    events = StageStore(path=tmp_path / "stages.yml").get_events("Parent")
    assert [e["stage"] for e in events] == ["intake", "research", "plan", "run", "verify"]


def test_run_error_logs(monkeypatch):
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_stage_update_event", lambda *a, **k: None
    )

    calls = []

    def fake_emit(task, stage, status, *, reason=None, user_id=None, group_id=None, **_):
        calls.append((task, stage, status, reason))

    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_audit_log", fake_emit
    )

    class BoomTask:
        def run(self):
            raise RuntimeError("boom")

    pipeline = TaskPipeline(BoomTask())
    with pytest.raises(RuntimeError, match="boom"):
        pipeline.run(user_id="alice")

    assert any(
        c[1] == "execute" and c[2] == "error" and c[3] == "boom" for c in calls
    )
