import pytest
import requests

from task_cascadence.plugins.cronyx_server import CronyxServerLoader


class DummyResponse:
    def raise_for_status(self):
        raise requests.HTTPError("boom", response=self)

    def json(self):
        return {}


def fake_get(url, timeout):
    return DummyResponse()


class SuccessResponse:
    def raise_for_status(self):
        pass

    def json(self):
        return {"ok": True}


def fake_success(url, timeout):
    return SuccessResponse()


class FailingGet:
    def __init__(self):
        self.calls = 0

    def __call__(self, url, timeout):
        self.calls += 1
        raise requests.ConnectionError("fail")


@pytest.mark.parametrize("method,args", [("list_tasks", ()), ("load_task", ("42",))])
def test_loader_http_error(monkeypatch, method, args):
    loader = CronyxServerLoader("http://server")
    monkeypatch.setattr(requests, "get", fake_get)
    with pytest.raises(requests.HTTPError):
        getattr(loader, method)(*args)


@pytest.mark.parametrize("method,args", [("list_tasks", ()), ("load_task", ("42",))])
def test_loader_success(monkeypatch, method, args):
    loader = CronyxServerLoader("http://server")
    monkeypatch.setattr(requests, "get", fake_success)
    assert getattr(loader, method)(*args) == {"ok": True}


def test_loader_retries(monkeypatch):
    failing = FailingGet()
    loader = CronyxServerLoader("http://server", retries=3, backoff_factor=0)
    monkeypatch.setattr(requests, "get", failing)
    monkeypatch.setattr("time.sleep", lambda s: None)
    with pytest.raises(requests.ConnectionError):
        loader.list_tasks()
    assert failing.calls == 3
