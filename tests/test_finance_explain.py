import sys
from pathlib import Path
import types

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.modules["task_cascadence.workflows.calendar_event_creation"] = types.ModuleType(
    "calendar_event_creation"
)
from task_cascadence.workflows import dispatch  # noqa: E402
from task_cascadence.workflows import financial_decision_support as fds  # noqa: E402


class DummyResponse:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


def _setup(monkeypatch):
    async def fake_request(method, url, timeout, **kwargs):
        if method == "GET":
            return DummyResponse({"nodes": []})
        elif url.endswith("/v1/simulations/debt"):
            return DummyResponse({"id": "da1"})
        else:
            return DummyResponse({"ok": True})

    monkeypatch.setattr(fds, "request_with_retry_async", fake_request)
    monkeypatch.setattr(fds, "emit_stage_update_event", lambda *a, **k: None)
    monkeypatch.setattr(fds, "emit_audit_log", lambda *a, **k: None)

    dispatched = []

    def fake_dispatch(event, payload, user_id, group_id=None):
        dispatched.append(event)

    monkeypatch.setattr(fds, "dispatch", fake_dispatch)
    return dispatched


def test_finance_explain_dispatched(monkeypatch):
    dispatched = _setup(monkeypatch)
    dispatch(
        "finance.decision.request",
        {"budget": 0, "max_options": 0, "explain": True},
        user_id="alice",
    )
    assert dispatched[0] == "finance.decision.result"
    assert "finance.explain.request" in dispatched


def test_finance_explain_omitted(monkeypatch):
    dispatched = _setup(monkeypatch)
    dispatch(
        "finance.decision.request",
        {"budget": 0, "max_options": 0},
        user_id="alice",
    )
    assert dispatched == ["finance.decision.result"]
