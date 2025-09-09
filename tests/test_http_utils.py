import asyncio
import pytest
import requests
import httpx
from task_cascadence.http_utils import request_with_retry, request_with_retry_async
import time


class DummyResponse:
    def __init__(self, ok=True):
        self.ok = ok

    def raise_for_status(self):
        if not self.ok:
            raise requests.HTTPError("bad", response=self)


def test_request_with_retry_success(monkeypatch):
    class FakeSession:
        def __init__(self):
            self.calls = 0
        def request(self, method, url, timeout=0, **kwargs):
            self.calls += 1
            assert timeout == 1.0
            if self.calls < 3:
                raise requests.ConnectionError("boom")
            return DummyResponse()

    sleeps = []
    monkeypatch.setattr(time, "sleep", lambda s: sleeps.append(s))
    session = FakeSession()
    resp = request_with_retry("GET", "http://x", timeout=1.0, retries=3, backoff_factor=0.1, session=session)
    assert isinstance(resp, DummyResponse)
    assert session.calls == 3
    assert len(sleeps) == 2


def test_request_with_retry_timeout(monkeypatch):
    class FakeSession:
        def __init__(self):
            self.calls = 0
        def request(self, method, url, timeout=0, **kwargs):
            self.calls += 1
            assert timeout == 0.5
            raise requests.Timeout("nope")
    monkeypatch.setattr(time, "sleep", lambda s: None)
    session = FakeSession()
    with pytest.raises(requests.Timeout):
        request_with_retry("GET", "http://x", timeout=0.5, retries=2, backoff_factor=0, session=session)
    assert session.calls == 2


@pytest.mark.asyncio
async def test_request_with_retry_async_success(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.calls = 0

        async def request(self, method, url, timeout=0, **kwargs):
            self.calls += 1
            assert timeout == 1.0
            if self.calls < 3:
                raise httpx.HTTPError("boom")
            return DummyResponse()

    sleeps: list[float] = []

    async def fake_sleep(s):
        sleeps.append(s)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    client = FakeClient()
    resp = await request_with_retry_async(
        "GET",
        "http://x",
        timeout=1.0,
        retries=3,
        backoff_factor=0.1,
        client=client,
    )
    assert isinstance(resp, DummyResponse)
    assert client.calls == 3
    assert len(sleeps) == 2


@pytest.mark.asyncio
async def test_request_with_retry_async_timeout(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.calls = 0

        async def request(self, method, url, timeout=0, **kwargs):
            self.calls += 1
            assert timeout == 0.5
            raise httpx.TimeoutException("nope")

    async def fake_sleep(_):
        pass

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    client = FakeClient()
    with pytest.raises(httpx.TimeoutException):
        await request_with_retry_async(
            "GET", "http://x", timeout=0.5, retries=2, backoff_factor=0, client=client
        )
    assert client.calls == 2
