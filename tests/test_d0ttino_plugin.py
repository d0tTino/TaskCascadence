import subprocess

import requests

from task_cascadence.plugins.d0tTino import D0tTinoTask


class DummyCompleted:
    def __init__(self, out: str) -> None:
        self.stdout = out


def test_cli_invocation(monkeypatch):
    captured = {}

    def fake_run(args, capture_output=False, text=False, check=False):
        captured["args"] = args
        return DummyCompleted("ok")

    monkeypatch.setattr(subprocess, "run", fake_run)

    task = D0tTinoTask("ping")
    result = task.run()

    assert result == "ok"
    assert captured["args"] == ["d0tTino", "ai", "ping"]


def test_api_invocation(monkeypatch):
    class Resp:
        def __init__(self, txt: str) -> None:
            self.text = txt

        def raise_for_status(self) -> None:
            pass

    captured = {}

    def fake_post(url, json=None, timeout=0):
        captured["url"] = url
        captured["json"] = json
        return Resp("plan")

    monkeypatch.setattr(requests, "post", fake_post)

    task = D0tTinoTask("data", use_api=True, base_url="http://api")
    result = task.plan()

    assert result == "plan"
    assert captured["url"] == "http://api/ai-plan"
    assert captured["json"] == {"prompt": "data"}
