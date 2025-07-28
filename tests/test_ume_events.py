import yaml
from typer.testing import CliRunner

from task_cascadence.orchestrator import TaskPipeline
from task_cascadence.cli import app
from task_cascadence.scheduler import BaseScheduler
from task_cascadence.plugins import ExampleTask
from task_cascadence.ume import _hash_user_id, emit_task_note, emit_idea_seed
from task_cascadence.ume.models import TaskNote, IdeaSeed, StageUpdate


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
    ume._stage_store = None

    pipeline = TaskPipeline(DemoTask())
    pipeline.run(user_id="alice")

    data = yaml.safe_load(path.read_text())
    events = data["DemoTask"]
    stages = [e["stage"] for e in events]
    assert stages == ["intake", "planning", "run", "verification"]
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
    result = runner.invoke(app, ["run", "example", "--user-id", "bob"])
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
    emit_task_note(note, client, user_id="alice")

    assert isinstance(client.events[0], TaskNote)
    assert client.events[0].user_hash == _hash_user_id("alice")
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
    emit_idea_seed(seed, client, user_id="bob")

    assert isinstance(client.events[0], IdeaSeed)
    assert client.events[0].user_hash == _hash_user_id("bob")
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

    ume.emit_stage_update_event("demo", "start", client, user_id="alice")

    assert isinstance(client.events[0], StageUpdate)
    assert client.events[0].user_hash == _hash_user_id("alice")
    serialized = client.events[0].SerializeToString()
    again = StageUpdate.FromString(serialized)
    assert again == client.events[0]

    data = yaml.safe_load((tmp_path / "stages.yml").read_text())
    assert data["demo"][0]["user_hash"] == _hash_user_id("alice")
