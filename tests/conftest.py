import sys
from types import ModuleType
from pathlib import Path

import pytest

# Ensure package root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Stub modules that have heavy deps before importing package
metrics_mod = ModuleType("task_cascadence.metrics")


def track_task(*args, **kwargs):
    def deco(fn):
        return fn
    return deco


metrics_mod.track_task = track_task
metrics_mod.start_metrics_server = lambda port=8000: None
sys.modules["task_cascadence.metrics"] = metrics_mod

temporal_mod = ModuleType("task_cascadence.temporal")


class TemporalBackend:  # minimal stub for tests
    pass


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
