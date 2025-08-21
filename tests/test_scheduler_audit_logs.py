import importlib.machinery
import importlib.util
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]


def _load(name: str, path: pathlib.Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    if "." in name:
        parent_name, attr = name.rsplit(".", 1)
        setattr(sys.modules[parent_name], attr, module)
    return module


spec_pkg = importlib.machinery.ModuleSpec(
    "task_cascadence", loader=None, is_package=True
)
spec_pkg.submodule_search_locations = [str(ROOT / "task_cascadence")]
pkg = importlib.util.module_from_spec(spec_pkg)
sys.modules.setdefault("task_cascadence", pkg)


_DEF_SCHED = ROOT / "task_cascadence" / "scheduler" / "__init__.py"
_DEF_UME = ROOT / "task_cascadence" / "ume" / "__init__.py"


def test_scheduler_audit_logs(monkeypatch, tmp_path):
    monkeypatch.setenv("CASCADENCE_STAGES_PATH", str(tmp_path / "stages.yml"))

    sched_mod = _load("task_cascadence.scheduler", _DEF_SCHED)
    ume_mod = _load("task_cascadence.ume", _DEF_UME)

    calls = []

    def fake_emit(task, stage, status, *, user_id=None, group_id=None, **_):
        if stage in {"scheduler", "unschedule"}:
            calls.append((task, stage, status, user_id, group_id))

    monkeypatch.setattr(ume_mod, "emit_audit_log", fake_emit)

    class Dummy:
        def run(self):
            pass

    sched = sched_mod.CronScheduler(storage_path=tmp_path / "sched.yml")
    sched.schedule_task(Dummy(), "* * * * *", user_id="alice", group_id="team")

    sched.disable_task("Dummy", user_id="alice", group_id="team")
    sched.pause_task("Dummy", user_id="alice", group_id="team")
    sched.resume_task("Dummy", user_id="alice", group_id="team")
    sched.unschedule("Dummy")

    assert calls == [
        ("Dummy", "scheduler", "disabled", "alice", "team"),
        ("Dummy", "scheduler", "paused", "alice", "team"),
        ("Dummy", "scheduler", "resumed", "alice", "team"),
        (
            "Dummy",
            "unschedule",
            "success",
            ume_mod._hash_user_id("alice"),
            "team",
        ),
    ]

