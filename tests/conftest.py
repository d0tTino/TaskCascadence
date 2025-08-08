import pytest
import importlib.util
import sys
from types import ModuleType
from pathlib import Path


# Load scheduler module directly without importing full package dependencies
pkg = ModuleType("task_cascadence")
sys.modules.setdefault("task_cascadence", pkg)
temporal_mod = ModuleType("task_cascadence.temporal")


class TemporalBackend:  # minimal stub for tests
    pass


temporal_mod.TemporalBackend = TemporalBackend
sys.modules["task_cascadence.temporal"] = temporal_mod
pkg.temporal = temporal_mod

metrics_mod = ModuleType("task_cascadence.metrics")


def track_task(*args, **kwargs):
    def deco(fn):
        return fn

    return deco


metrics_mod.track_task = track_task
sys.modules["task_cascadence.metrics"] = metrics_mod
pkg.metrics = metrics_mod

http_spec = importlib.util.spec_from_file_location(
    "task_cascadence.http_utils",
    Path(__file__).resolve().parents[1] / "task_cascadence" / "http_utils.py",
)
http_utils = importlib.util.module_from_spec(http_spec)
sys.modules["task_cascadence.http_utils"] = http_utils
http_spec.loader.exec_module(http_utils)  # type: ignore[union-attr]
pkg.http_utils = http_utils
spec = importlib.util.spec_from_file_location(
    "task_cascadence.scheduler",
    Path(__file__).resolve().parents[1]
    / "task_cascadence"
    / "scheduler"
    / "__init__.py",
)
scheduler_module = importlib.util.module_from_spec(spec)
sys.modules["task_cascadence.scheduler"] = scheduler_module
spec.loader.exec_module(scheduler_module)  # type: ignore[union-attr]
pkg.scheduler = scheduler_module


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
