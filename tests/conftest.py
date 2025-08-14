import sys
import os
import asyncio
from types import ModuleType
from pathlib import Path

import pytest

# Ensure package root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Provide a lightweight TemporalBackend stub before importing package

temporal_mod = ModuleType("task_cascadence.temporal")


class Client:  # minimal stub used by TemporalBackend
    @staticmethod
    async def connect(server: str, **kwargs):  # pragma: no cover - patched in tests
        raise NotImplementedError


class Replayer:  # minimal stub used by TemporalBackend
    def replay(self, path: str):  # pragma: no cover - patched in tests
        raise NotImplementedError


class TemporalBackend:  # minimal stub for tests
    def __init__(self) -> None:
        self.server = os.getenv("TEMPORAL_SERVER", "localhost:7233")

    def run_workflow_sync(self, workflow, *args, **kwargs):  # pragma: no cover - patched in tests
        async def _run():
            client = await Client.connect(self.server)
            return await client.execute_workflow(workflow, *args, **kwargs)

        return asyncio.run(_run())

    def replay(self, path):  # pragma: no cover - patched in tests
        replayer_cls = sys.modules["task_cascadence.temporal"].Replayer
        return replayer_cls().replay(path)


temporal_mod.TemporalBackend = TemporalBackend
temporal_mod.Client = Client
temporal_mod.Replayer = Replayer
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
    class DummyClient:
        def enqueue(self, obj):
            pass

    monkeypatch.setattr("task_cascadence.ume._default_client", DummyClient())
