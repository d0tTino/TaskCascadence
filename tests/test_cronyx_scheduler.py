import importlib
import os
import requests
import pytest

import task_cascadence
from task_cascadence.scheduler.cronyx import CronyxScheduler
from task_cascadence.scheduler import create_scheduler


class DummyResp:
    def __init__(self, data):
        self.data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self.data


def test_run_task_posts_job(monkeypatch):
    captured = {}

    def fake_request(method, url, timeout=0, **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured["json"] = kwargs.get("json")
        return DummyResp({"result": "ok"})

    monkeypatch.setattr(requests, "request", fake_request)
    sched = CronyxScheduler(base_url="http://server")
    result = sched.run_task("demo", user_id="bob")
    assert result == "ok"
    assert captured["method"] == "POST"
    assert captured["url"] == "http://server/jobs"
    from task_cascadence.ume import _hash_user_id

    assert captured["json"] == {"task": "demo", "user_id": _hash_user_id("bob")}


def test_schedule_task_posts_job(monkeypatch):
    captured = {}

    def fake_request(method, url, timeout=0, **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured["json"] = kwargs.get("json")
        return DummyResp({"scheduled": True})

    monkeypatch.setattr(requests, "request", fake_request)
    sched = CronyxScheduler(base_url="http://server")
    resp = sched.schedule_task("demo", "* * * * *", user_id="alice")
    assert resp == {"scheduled": True}
    from task_cascadence.ume import _hash_user_id

    assert captured["json"] == {
        "task": "demo",
        "cron": "* * * * *",
        "user_id": _hash_user_id("alice"),
    }


def test_create_scheduler_factory():
    os.environ["CRONYX_BASE_URL"] = "http://server"
    sched = create_scheduler("cronyx")
    os.environ.pop("CRONYX_BASE_URL")
    assert isinstance(sched, CronyxScheduler)


def test_env_selects_cronyx(monkeypatch):
    monkeypatch.setenv("CASCADENCE_SCHEDULER", "cronyx")
    monkeypatch.setenv("CRONYX_BASE_URL", "http://server")
    monkeypatch.setattr(task_cascadence.plugins, "initialize", lambda: None)
    monkeypatch.setattr(task_cascadence.plugins, "load_cronyx_tasks", lambda: None)
    importlib.reload(task_cascadence)
    task_cascadence.initialize()
    from task_cascadence.scheduler import get_default_scheduler

    assert isinstance(get_default_scheduler(), CronyxScheduler)


def test_request_retries_on_unreachable(monkeypatch):
    calls = 0

    def fail_request(method, url, timeout=0, **kwargs):
        nonlocal calls
        calls += 1
        raise requests.Timeout("unreachable")

    monkeypatch.setattr(requests, "request", fail_request)
    monkeypatch.setattr("task_cascadence.http_utils.time.sleep", lambda s: None)
    sched = CronyxScheduler(base_url="http://server", retries=2, backoff_factor=0)
    with pytest.raises(requests.Timeout):
        sched._request("GET", "/jobs")
    assert calls == 2


def test_request_propagates_timeout(monkeypatch):
    captured = {}

    def fake_rwr(method, url, *, timeout, retries, backoff_factor, json=None):
        captured["timeout"] = timeout
        captured["retries"] = retries
        captured["backoff_factor"] = backoff_factor
        raise requests.Timeout("boom")

    monkeypatch.setattr(
        "task_cascadence.scheduler.cronyx.request_with_retry",
        fake_rwr,
    )
    sched = CronyxScheduler(base_url="http://server", timeout=3.5, retries=4, backoff_factor=0.2)
    with pytest.raises(requests.Timeout):
        sched._request("POST", "/jobs", json={"a": 1})
    assert captured["retries"] == 4
    assert captured["timeout"] == 3.5

