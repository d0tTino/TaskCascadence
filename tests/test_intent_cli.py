import builtins
from typer.testing import CliRunner

import requests

from d0tTino_cli import app


class Resp:
    def __init__(self, data: dict) -> None:
        self._data = data

    def json(self) -> dict:
        return self._data

    def raise_for_status(self) -> None:  # pragma: no cover - nothing to raise
        pass


def test_clarification_flow(monkeypatch):
    calls: list[dict] = []
    responses = [
        Resp({"clarification": "Which account?"}),
        Resp({"tasks": ["Task A"]}),
    ]

    def fake_post(url, json=None, timeout=0):
        calls.append(dict(json))
        return responses[len(calls) - 1]

    monkeypatch.setattr(requests, "post", fake_post)
    monkeypatch.setattr(builtins, "input", lambda prompt="": "Personal")

    runner = CliRunner()
    result = runner.invoke(app, ["pay bills"])

    assert result.exit_code == 0
    assert "Which account?" in result.stdout
    assert "Derived tasks:" in result.stdout
    assert "Task A" in result.stdout
    assert calls == [
        {"message": "pay bills", "context": []},
        {"message": "pay bills", "context": [], "clarification": "Personal"},
    ]


def test_success_flow(monkeypatch):
    calls: list[dict] = []

    def fake_post(url, json=None, timeout=0):
        calls.append(json)
        return Resp({"tasks": ["Task B"]})

    monkeypatch.setattr(requests, "post", fake_post)

    runner = CliRunner()
    result = runner.invoke(app, ["buy milk"])

    assert result.exit_code == 0
    assert "Task B" in result.stdout
    assert calls == [{"message": "buy milk", "context": []}]
