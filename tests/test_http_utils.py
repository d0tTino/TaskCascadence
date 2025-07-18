import pytest
import requests
from task_cascadence.http_utils import request_with_retry
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
