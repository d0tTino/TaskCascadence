import sys
from types import ModuleType
from pathlib import Path

import pytest

# Ensure package root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Provide a lightweight TemporalBackend stub before importing package
temporal_mod = ModuleType("task_cascadence.temporal")


class TemporalBackend:  # minimal stub for tests
    def run_workflow_sync(self, workflow):  # pragma: no cover - patched in tests
        raise NotImplementedError

    def replay(self, path):  # pragma: no cover - patched in tests
        raise NotImplementedError


temporal_mod.TemporalBackend = TemporalBackend
sys.modules["task_cascadence.temporal"] = temporal_mod

# Import actual package now that stubs are in place
import task_cascadence as pkg  # noqa: E402

scheduler_module = pkg.scheduler


@pytest.fixture(autouse=True)
def shutdown_scheduler():
    yield
    sched = getattr(scheduler_module, "_default_scheduler", None)
    if sched and hasattr(sched, "shutdown"):
        try:
            sched.shutdown(wait=False)
        except Exception:
            pass
    scheduler_module._default_scheduler = None


@pytest.fixture(autouse=True)
def tmp_pointers(monkeypatch, tmp_path):
    path = tmp_path / "pointers.yml"
    monkeypatch.setenv("CASCADENCE_POINTERS_PATH", str(path))
    yield


@pytest.fixture(autouse=True)
def stub_ume(monkeypatch):
    """Stub UME emission functions to avoid needing a transport client."""
    monkeypatch.setattr("task_cascadence.ume.emit_task_spec", lambda *a, **k: None)
    monkeypatch.setattr("task_cascadence.ume.emit_task_run", lambda *a, **k: None)
    monkeypatch.setattr(
        "task_cascadence.ume.emit_stage_update_event", lambda *a, **k: None
    )
    monkeypatch.setattr("task_cascadence.ume.emit_audit_log", lambda *a, **k: None)
