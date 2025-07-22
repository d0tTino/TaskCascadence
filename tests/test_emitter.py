from datetime import datetime
import threading
import time
import asyncio

import pytest


from task_cascadence.ume import emit_task_run, emit_task_spec
from task_cascadence.ume.models import TaskRun, TaskSpec


class MockClient:
    def __init__(self):
        self.events = []

    def enqueue(self, obj):
        self.events.append((obj, time.monotonic()))


class AsyncMockClient:
    def __init__(self):
        self.events = []

    async def enqueue(self, obj):
        self.events.append((obj, asyncio.get_running_loop().time()))


class SlowAsyncClient:
    async def enqueue(self, obj):
        await asyncio.sleep(0.3)


def test_emit_task_run_within_deadline():
    client = MockClient()
    spec = TaskSpec(id="1", name="sample")
    run = TaskRun(
        spec=spec,
        run_id="run1",
        status="success",
        started_at=datetime.now(),
        finished_at=datetime.now(),
    )

    start = time.monotonic()
    emit_task_run(run, client)
    assert client.events
    queued, ts = client.events[0]
    assert isinstance(queued, TaskRun)
    assert queued == run
    delay = ts - start
    assert delay < 0.2


def test_emit_task_spec_within_deadline():
    client = MockClient()
    spec = TaskSpec(id="2", name="other")
    start = time.monotonic()
    emit_task_spec(spec, client)
    assert client.events
    queued, ts = client.events[0]
    assert isinstance(queued, TaskSpec)
    assert queued == spec
    delay = ts - start
    assert delay < 0.2


class SlowClient:
    def enqueue(self, obj):
        time.sleep(0.3)


def test_emit_timeout_no_lingering_threads():
    client = SlowClient()
    spec = TaskSpec(id="3", name="timeout")
    run = TaskRun(
        spec=spec,
        run_id="run3",
        status="success",
        started_at=datetime.now(),
        finished_at=datetime.now(),
    )

    before = threading.active_count()
    with pytest.raises(RuntimeError):
        emit_task_run(run, client)
    # allow the daemon thread to finish
    time.sleep(0.35)
    after = threading.active_count()
    assert after == before


def test_emit_timeout_elapsed(monkeypatch):
    client = MockClient()
    spec = TaskSpec(id="4", name="elapsed")
    times = [0.0, 0.25]

    def fake_monotonic():
        return times.pop(0) if times else 0.25

    monkeypatch.setattr(time, "monotonic", fake_monotonic)
    with pytest.raises(RuntimeError, match="took"):
        emit_task_spec(spec, client)


def test_async_emit_task_spec_and_run():
    client = AsyncMockClient()
    spec = TaskSpec(id="5", name="async")
    run = TaskRun(
        spec=spec,
        run_id="rasync",
        status="ok",
        started_at=datetime.now(),
        finished_at=datetime.now(),
    )

    async def runner():
        t1 = emit_task_spec(spec, client, use_asyncio=True)
        t2 = emit_task_run(run, client, use_asyncio=True)
        await asyncio.gather(t1, t2)

    asyncio.run(runner())

    assert len(client.events) == 2
    assert isinstance(client.events[0][0], TaskSpec)
    assert isinstance(client.events[1][0], TaskRun)


def test_async_emit_timeout():
    client = SlowAsyncClient()
    spec = TaskSpec(id="6", name="slow")

    async def runner():
        task = emit_task_spec(spec, client, use_asyncio=True)
        await task

    with pytest.raises(RuntimeError):
        asyncio.run(runner())
