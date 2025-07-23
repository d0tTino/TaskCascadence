import pytest
import requests
import logging

from task_cascadence.plugins.cronyx_server import CronyxServerLoader


class DummyResponse:
    def raise_for_status(self):
        raise requests.HTTPError("boom", response=self)

    def json(self):
        return {}


def fake_request(method, url, timeout=0, **kwargs):
    return DummyResponse()


class SuccessResponse:
    def raise_for_status(self):
        pass

    def json(self):
        return {"ok": True}


def fake_success_request(method, url, timeout=0, **kwargs):
    return SuccessResponse()


class FailingRequest:
    def __init__(self):
        self.calls = 0

    def __call__(self, method, url, timeout=0, **kwargs):
        self.calls += 1
        raise requests.ConnectionError("fail")


@pytest.mark.parametrize("method,args", [("list_tasks", ()), ("load_task", ("42",))])
def test_loader_http_error(monkeypatch, method, args):
    loader = CronyxServerLoader("http://server")
    monkeypatch.setattr(requests, "request", fake_request)
    with pytest.raises(requests.HTTPError):
        getattr(loader, method)(*args)


@pytest.mark.parametrize("method,args", [("list_tasks", ()), ("load_task", ("42",))])
def test_loader_success(monkeypatch, method, args):
    loader = CronyxServerLoader("http://server")
    monkeypatch.setattr(requests, "request", fake_success_request)
    assert getattr(loader, method)(*args) == {"ok": True}


def test_loader_retries(monkeypatch):
    failing = FailingRequest()
    loader = CronyxServerLoader("http://server", retries=3, backoff_factor=0)
    monkeypatch.setattr(requests, "request", failing)
    monkeypatch.setattr("time.sleep", lambda s: None)
    assert loader.list_tasks() == []
    assert failing.calls == 3


def test_loader_connection_error(monkeypatch, caplog):
    def fail(*args, **kwargs):
        raise requests.ConnectionError("boom")

    loader = CronyxServerLoader("http://server")
    monkeypatch.setattr(
        "task_cascadence.plugins.cronyx_server.request_with_retry",
        fail,
    )

    with caplog.at_level(logging.WARNING):
        assert loader.list_tasks() == []
        assert loader.load_task("42") == {}

    records = [r for r in caplog.records if "Failed to reach CronyxServer" in r.getMessage()]
    assert len(records) == 2
    assert all(r.levelno >= logging.WARNING for r in records)


def test_loader_timeout_from_env(monkeypatch):
    class Recorder:
        def __init__(self):
            self.timeout = None

        def __call__(self, method, url, timeout=0, **kwargs):
            self.timeout = timeout
            return SuccessResponse()

    recorder = Recorder()
    monkeypatch.setenv("CRONYX_TIMEOUT", "7.5")
    monkeypatch.setattr(requests, "request", recorder)
    loader = CronyxServerLoader("http://server")
    assert loader.list_tasks() == {"ok": True}
    assert recorder.timeout == 7.5


def test_list_tasks_parsed(monkeypatch):
    class Resp:
        def __init__(self, data):
            self._data = data

        def raise_for_status(self):
            pass

        def json(self):
            return self._data

    expected = [{"id": "1"}]

    def fake_get(url, timeout=0, **kwargs):
        return Resp(expected)

    def fake_request(method, url, timeout=0, **kwargs):
        assert method == "GET"
        return fake_get(url, timeout=timeout, **kwargs)

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(requests, "request", fake_request)

    loader = CronyxServerLoader("http://server")
    assert loader.list_tasks() == expected


def test_load_task_parsed(monkeypatch):
    class Resp:
        def __init__(self, data):
            self._data = data

        def raise_for_status(self):
            pass

        def json(self):
            return self._data

    expected = {"id": "1", "path": "module:Cls"}

    def fake_get(url, timeout=0, **kwargs):
        return Resp(expected)

    def fake_request(method, url, timeout=0, **kwargs):
        assert method == "GET"
        return fake_get(url, timeout=timeout, **kwargs)

    monkeypatch.setattr(requests, "get", fake_get)
    monkeypatch.setattr(requests, "request", fake_request)

    loader = CronyxServerLoader("http://server")
    assert loader.load_task("1") == expected
