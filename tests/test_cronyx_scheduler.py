import importlib
import os
import requests

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
    assert captured["json"] == {"task": "demo", "user_id": "bob"}


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
    assert captured["json"] == {"task": "demo", "cron": "* * * * *", "user_id": "alice"}


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

