import pytest

import task_cascadence.scheduler as scheduler_module


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
