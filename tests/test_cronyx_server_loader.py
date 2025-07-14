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


@pytest.mark.parametrize("method,args", [("list_tasks", ()), ("load_task", ("42",))])
def test_loader_http_error(monkeypatch, method, args):
    loader = CronyxServerLoader("http://server")
    monkeypatch.setattr(requests, "get", fake_get)
    with pytest.raises(requests.HTTPError):
        getattr(loader, method)(*args)
