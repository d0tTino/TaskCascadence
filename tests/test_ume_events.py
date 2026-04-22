import yaml
from typer.testing import CliRunner

from task_cascadence.orchestrator import TaskPipeline
from task_cascadence.cli import app
from task_cascadence.scheduler import BaseScheduler
from task_cascadence.plugins import ExampleTask
from task_cascadence.ume import _hash_user_id, emit_task_note, emit_idea_seed
from task_cascadence.ume.models import IdeaSeed, TaskNote
from task_cascadence.ume.schema_public import SCHEMA_VERSION


class DemoTask:
    def intake(self):
        pass

    def plan(self):
        return None

    def run(self):
        return "ok"

    def verify(self, result):
        return result


def test_pipeline_stage_events(monkeypatch, tmp_path):
    path = tmp_path / "stages.yml"
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(path))
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")

    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_task_spec", lambda *a, **k: None
    )
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_task_run", lambda *a, **k: None
    )
    import task_cascadence.ume as ume
    from task_cascadence.stage_store import StageStore

    # Ensure the stage store uses our temporary path and the file exists, and
    # restore the orchestrator's stage event emitter in case earlier tests
    # patched it.
    ume._stage_store = StageStore(path)
    monkeypatch.setattr(
        "task_cascadence.orchestrator.emit_stage_update_event",
        ume.emit_stage_update_event,
    )

    pipeline = TaskPipeline(DemoTask())
    pipeline.run(user_id="alice")

    data = yaml.safe_load(path.read_text())
    events = data["DemoTask"]
    stages = [e["stage"] for e in events]
    assert stages == ["intake", "research", "plan", "run", "verify"]
    for e in events:
        assert e["user_hash"] == _hash_user_id("alice")


def test_cli_stage_events(monkeypatch, tmp_path):
    path = tmp_path / "stages.yml"
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(path))
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")

    sched = BaseScheduler()
    sched.register_task("example", ExampleTask())
    monkeypatch.setattr("task_cascadence.cli.get_default_scheduler", lambda: sched)

    monkeypatch.setattr(
        "task_cascadence.ume.emit_task_run", lambda *a, **k: None
    )
    import task_cascadence.ume as ume
    ume._stage_store = None

    runner = CliRunner()
    result = runner.invoke(
        app,
        ["run", "example", "--user-id", "bob", "--group-id", "ops"],
    )
    assert result.exit_code == 0

    data = yaml.safe_load(path.read_text())
    events = data["example"]
    assert events[0]["stage"] == "start"
    assert events[-1]["stage"] == "finish"
    assert events[0]["user_hash"] == _hash_user_id("bob")


def test_emit_task_note(monkeypatch):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")

    class Client:
        def __init__(self):
            self.events = []

        def enqueue(self, obj):
            self.events.append(obj)

    client = Client()
    note = TaskNote(task_name="demo", run_id="r1", note="all good")
    emit_task_note(note, client, user_id="alice", group_id="devs")

    assert isinstance(client.events[0], TaskNote)
    assert client.events[0].user_hash == _hash_user_id("alice")
    assert client.events[0].user_id == "alice"
    assert client.events[0].group_id == "devs"
    serialized = client.events[0].SerializeToString()
    again = TaskNote.FromString(serialized)
    assert again == client.events[0]


def test_emit_idea_seed(monkeypatch):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")

    class Client:
        def __init__(self):
            self.events = []

        def enqueue(self, obj):
            self.events.append(obj)

    client = Client()
    seed = IdeaSeed(text="an idea")
    emit_idea_seed(seed, client, user_id="bob", group_id="devs")

    assert isinstance(client.events[0], IdeaSeed)
    assert client.events[0].user_hash == _hash_user_id("bob")
    assert client.events[0].user_id == "bob"
    assert client.events[0].group_id == "devs"
    serialized = client.events[0].SerializeToString()
    again = IdeaSeed.FromString(serialized)
    assert again == client.events[0]


def test_emit_stage_update_event(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "stages.yml"))

    class Client:
        def __init__(self):
            self.events = []

        def enqueue(self, obj):
            self.events.append(obj)

    client = Client()

    import task_cascadence.ume as ume
    ume._stage_store = None

    ume.emit_stage_update_event("demo", "start", client, user_id="alice", group_id="devs")

    payload = client.events[0]
    assert isinstance(payload, dict)
    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["event_type"] == "TASK.STAGE.STARTED"
    assert payload["stage"] == "start"  # compatibility for legacy consumers
    assert payload["user_hash"] == _hash_user_id("alice")
    assert payload["user_id"] == "alice"
    assert payload["group_id"] == "devs"

    data = yaml.safe_load((tmp_path / "stages.yml").read_text())
    assert data["demo"][0]["user_hash"] == _hash_user_id("alice")


def test_emit_stage_update_event_default_client(monkeypatch, tmp_path):
    """StageUpdate events include a hashed user ID when using the default client."""
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "events.yml"))

    class Client:
        def __init__(self) -> None:
            self.events = []

        def enqueue(self, obj) -> None:
            self.events.append(obj)

    import task_cascadence.ume as ume
    ume._stage_store = None
    client = Client()
    monkeypatch.setattr(ume, "_default_client", client)

    ume.emit_stage_update_event("demo", "plan", user_id="bob", group_id="devs")

    payload = client.events[0]
    assert isinstance(payload, dict)
    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["event_type"] == "TASK.STAGE.UPDATED"
    assert payload["stage"] == "plan"
    assert payload["user_hash"] == _hash_user_id("bob")
    assert payload["user_id"] == "bob"
    assert payload["group_id"] == "devs"
    data = yaml.safe_load((tmp_path / "events.yml").read_text())
    assert data["demo"][0]["user_hash"] == _hash_user_id("bob")


def test_emit_audit_log_emits_event(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "audit.yml"))

    class Client:
        def __init__(self) -> None:
            self.events = []

        def enqueue(self, obj) -> None:
            self.events.append(obj)

    import task_cascadence.ume as ume
    ume._stage_store = None
    client = Client()

    ume.emit_audit_log(
        "demo",
        "run",
        "success",
        client,
        user_id="carol",
        group_id="devs",
        output="done",
    )

    payload = client.events[0]
    assert isinstance(payload, dict)
    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["event_type"] == "TASK.STAGE.SUCCESS"
    assert payload["stage"] == "run"  # compatibility for legacy consumers
    assert payload["status"] == "success"  # compatibility for legacy consumers
    assert payload["user_hash"] == _hash_user_id("carol")
    assert payload["user_id"] == "carol"
    assert payload["group_id"] == "devs"
    assert payload["output"] == "done"

    data = yaml.safe_load((tmp_path / "audit.yml").read_text())
    key = "demo:audit"
    assert data[key][0]["user_hash"] == _hash_user_id("carol")
    assert data[key][0]["status"] == "success"


def test_emit_audit_log_records_failure(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    path = tmp_path / "audit.yml"
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(path))

    class Client:
        def __init__(self) -> None:
            self.events = []

        def enqueue(self, obj) -> None:
            self.events.append(obj)

    import task_cascadence.ume as ume
    ume._stage_store = None
    client = Client()

    ume.emit_audit_log(
        "demo",
        "run",
        "failure",
        client,
        user_id="dave",
        group_id="ops",
        reason="bad",
        output="oops",
    )

    payload = client.events[0]
    assert isinstance(payload, dict)
    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["event_type"] == "TASK.STAGE.ERROR"
    assert payload["status"] == "failure"
    assert payload["reason"] == "bad"
    assert payload["output"] == "oops"
    assert payload["user_hash"] == _hash_user_id("dave")
    assert payload["group_id"] == "ops"

    from task_cascadence.stage_store import StageStore

    store = StageStore(path=path)
    events = store.get_events(
        "demo",
        _hash_user_id("dave"),
        "ops",
        category="audit",
    )
    assert events[0]["status"] == "failure"
    assert events[0]["reason"] == "bad"
    assert events[0]["output"] == "oops"


def test_context_attach_audit_event_contract(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_HASH_SECRET", "s")
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "audit.yml"))

    class Client:
        def __init__(self) -> None:
            self.events = []

        def enqueue(self, obj) -> None:
            self.events.append(obj)

    import task_cascadence.ume as ume
    ume._stage_store = None
    client = Client()

    ume.emit_audit_log(
        "demo",
        "context_attached",
        "received",
        client,
        user_id="eve",
        group_id="ops",
    )

    payload = client.events[0]
    assert payload["schema_version"] == SCHEMA_VERSION
    assert payload["event_type"] == "CONTEXT.ATTACHED"
    assert payload["stage"] == "context_attached"
    assert payload["status"] == "received"
