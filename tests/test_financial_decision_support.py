import sys
from pathlib import Path
from typing import Any

import pytest
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from task_cascadence.workflows import dispatch
from task_cascadence.workflows import (
    financial_decision_support as fds,
)

class DummyResponse:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


def test_financial_decision_support(monkeypatch):
    calls: list[tuple[str, str, dict[str, Any]]] = []

    def fake_request(method, url, timeout, **kwargs):
        calls.append((method, url, kwargs))
        if method == "GET":
            assert kwargs["params"]["user_id"] == "alice"
            assert kwargs["params"]["group_id"] == "g1"
            return DummyResponse(
                {
                    "nodes": [
                        {"id": "acct1", "type": "FinancialAccount", "balance": 1000},
                        {"id": "acct2", "type": "FinancialAccount", "balance": 500},
                        {"id": "goal1", "type": "FinancialGoal", "target": 2000},
                        {"id": "da0", "type": "DecisionAnalysis"},
                    ]
                }
            )
        elif url.endswith("/v1/simulations/debt"):
            assert kwargs["json"]["balance"] == 1500
            assert len(kwargs["json"]["goals"]) == 1
            assert len(kwargs["json"]["analyses"]) == 1
            assert kwargs["json"]["budget"] == 100
            assert kwargs["json"]["max_options"] == 3
            assert kwargs["json"]["user_id"] == "alice"
            assert kwargs["json"]["group_id"] == "g1"
            return DummyResponse(
                {
                    "id": "da1",
                    "cost_of_deviation": 50,
                    "actions": [
                        {"id": "act1", "cost_of_deviation": 20},
                    ],
                }
            )
        else:  # persistence call
            return DummyResponse({"ok": True})

    emitted = {}

    def fake_emit(name, stage, user_id=None, group_id=None, **_):
        emitted["event"] = (name, stage, user_id, group_id)

    dispatched = []

    def fake_dispatch(event, payload, user_id, group_id=None):
        dispatched.append((event, payload, user_id, group_id))

    audit_logs = []

    def fake_audit_log(task_name, stage, status, *, reason=None, output=None, user_id=None, group_id=None, **_):
        audit_logs.append((task_name, stage, status, reason, output, user_id, group_id))

    monkeypatch.setattr(fds, "request_with_retry", fake_request)
    monkeypatch.setattr(fds, "emit_stage_update_event", fake_emit)
    monkeypatch.setattr(fds, "dispatch", fake_dispatch)
    monkeypatch.setattr(fds, "emit_audit_log", fake_audit_log)

    result = dispatch(
        "finance.decision.request",
        {"explain": True, "budget": 100, "max_options": 3},
        user_id="alice",
        group_id="g1",
    )

    # ensure UME query
    assert calls[0][0] == "GET"
    assert "FinancialAccount" in calls[0][2]["params"]["types"]

    # engine call
    assert calls[1][0] == "POST"
    assert calls[1][1].endswith("/v1/simulations/debt")

    # persistence call with metrics and edges
    persist = calls[2][2]["json"]
    assert all(n["user_id"] == "alice" and n["group_id"] == "g1" for n in persist["nodes"])
    assert any(
        n["type"] == "DecisionAnalysis" and n["metrics"]["cost_of_deviation"] == 50
        for n in persist["nodes"]
    )
    assert any(
        n["type"] == "ProposedAction" and n["metrics"]["cost_of_deviation"] == 20
        for n in persist["nodes"]
    )
    assert all(e["user_id"] == "alice" and e["group_id"] == "g1" for e in persist["edges"])
    assert any(
        e["src"] == "da1" and e["dst"] == "act1" and e["type"] == "CONSIDERS"
        for e in persist["edges"]
    )
    assert any(
        e["src"] == "act1" and e["dst"] == "acct1" and e["type"] == "OWNED_BY"
        for e in persist["edges"]
    )
    assert any(
        e["src"] == "act1" and e["dst"] == "acct2" and e["type"] == "OWNED_BY"
        for e in persist["edges"]
    )
    assert emitted["event"] == ("finance.decision.result", "completed", "alice", "g1")
    assert dispatched[0][0] == "finance.decision.result"
    assert dispatched[0][1]["summary"]["cost_of_deviation"] == 50
    assert dispatched[0][2] == "alice"
    assert dispatched[0][3] == "g1"
    assert dispatched[1][0] == "finance.explain.request"
    assert dispatched[1][1]["actions"][0]["id"] == "act1"

    assert result["analysis"] == "da1"
    assert result["summary"]["cost_of_deviation"] == 50

    assert ("finance.decision", "workflow", "started", None, None, "alice", "g1") in audit_logs
    assert ("finance.decision", "workflow", "completed", None, None, "alice", "g1") in audit_logs


def test_financial_decision_support_group_id_mismatch(monkeypatch):
    def fake_request(*a, **k):
        raise AssertionError("request should not be called")

    monkeypatch.setattr(fds, "request_with_retry", fake_request)
    monkeypatch.setattr(fds, "emit_stage_update_event", lambda *a, **k: None)
    monkeypatch.setattr(fds, "dispatch", lambda *a, **k: None)
    monkeypatch.setattr(fds, "emit_audit_log", lambda *a, **k: None)

    with pytest.raises(ValueError):
        dispatch(
            "finance.decision.request",
            {"group_id": "g2", "budget": 0, "max_options": 0},
            user_id="alice",
            group_id="g1",
        )


def test_financial_decision_support_time_horizon(monkeypatch):
    calls = []

    def fake_request(method, url, timeout, **kwargs):
        calls.append((method, url, kwargs))
        if method == "GET":
            return DummyResponse({"nodes": []})
        elif url.endswith("/v1/simulations/debt"):
            return DummyResponse({"id": "da1", "actions": []})
        else:
            return DummyResponse({"ok": True})

    monkeypatch.setattr(fds, "request_with_retry", fake_request)
    monkeypatch.setattr(fds, "emit_stage_update_event", lambda *a, **k: None)
    monkeypatch.setattr(fds, "dispatch", lambda *a, **k: None)
    monkeypatch.setattr(fds, "emit_audit_log", lambda *a, **k: None)

    dispatch(
        "finance.decision.request",
        {"time_horizon": "6m", "budget": 0, "max_options": 0},
        user_id="alice",
    )

    assert calls[0][2]["params"]["time_horizon"] == "6m"
    persist = calls[2][2]["json"]
    analysis = next(n for n in persist["nodes"] if n["type"] == "DecisionAnalysis")
    assert analysis["metadata"]["time_horizon"] == "6m"


def test_financial_decision_support_ume_error(monkeypatch):
    calls = []

    def fake_request(method, url, timeout, **kwargs):
        calls.append((method, url, kwargs))
        if method == "GET":
            raise requests.HTTPError("boom")
        return DummyResponse({})

    emitted = []

    def fake_emit(name, stage, user_id=None, group_id=None, **_):
        emitted.append((name, stage, user_id, group_id))

    dispatched = []

    def fake_dispatch(event, payload, user_id, group_id=None):
        dispatched.append(event)

    monkeypatch.setattr(fds, "request_with_retry", fake_request)
    monkeypatch.setattr(fds, "emit_stage_update_event", fake_emit)
    monkeypatch.setattr(fds, "dispatch", fake_dispatch)
    monkeypatch.setattr(fds, "emit_audit_log", lambda *a, **k: None)

    with pytest.raises(requests.HTTPError):
        dispatch(
            "finance.decision.request",
            {"budget": 100, "max_options": 3},
            user_id="alice",
        )

    assert len(calls) == 1
    assert emitted == [("finance.decision.result", "error", "alice", None)]
    assert "finance.decision.result" not in dispatched


def test_financial_decision_support_engine_failure(monkeypatch):
    calls = []

    def fake_request(method, url, timeout, **kwargs):
        calls.append((method, url, kwargs))
        if method == "GET":
            return DummyResponse({"nodes": []})
        elif url.endswith("/v1/simulations/debt"):
            raise requests.HTTPError("boom")
        return DummyResponse({})

    emitted = []

    def fake_emit(name, stage, user_id=None, group_id=None, **_):
        emitted.append((name, stage, user_id, group_id))

    dispatched = []

    def fake_dispatch(event, payload, user_id, group_id=None):
        dispatched.append(event)

    audit_logs: list[tuple[str, str, str, str, str | None, str | None, str | None]] = []

    def fake_audit_log(task_name, stage, status, *, reason=None, output=None, user_id=None, group_id=None, **_):
        audit_logs.append((task_name, stage, status, reason, output, user_id, group_id))

    monkeypatch.setattr(fds, "request_with_retry", fake_request)
    monkeypatch.setattr(fds, "emit_stage_update_event", fake_emit)
    monkeypatch.setattr(fds, "dispatch", fake_dispatch)
    monkeypatch.setattr(fds, "emit_audit_log", fake_audit_log)

    with pytest.raises(requests.HTTPError):
        dispatch(
            "finance.decision.request",
            {"budget": 100, "max_options": 3},
            user_id="alice",
        )

    assert len(calls) == 2
    assert emitted == [("finance.decision.result", "error", "alice", None)]
    assert "finance.decision.result" not in dispatched
    assert any(a[1] == "engine" and a[2] == "error" and "boom" in a[3] and "budget" in a[4] for a in audit_logs)
    assert any(a[1] == "workflow" and a[2] == "error" for a in audit_logs)


def test_financial_decision_support_persistence_failure(monkeypatch):
    calls = []

    def fake_request(method, url, timeout, **kwargs):
        calls.append((method, url, kwargs))
        if method == "GET":
            return DummyResponse({"nodes": []})
        elif url.endswith("/v1/simulations/debt"):
            return DummyResponse({"id": "da1", "actions": []})
        else:
            raise requests.HTTPError("boom")

    emitted = []

    def fake_emit(name, stage, user_id=None, group_id=None, **_):
        emitted.append((name, stage, user_id, group_id))

    dispatched = []

    def fake_dispatch(event, payload, user_id, group_id=None):
        dispatched.append(event)

    audit_logs = []

    def fake_audit_log(task_name, stage, status, *, reason=None, output=None, user_id=None, group_id=None, **_):
        audit_logs.append((task_name, stage, status, reason, output, user_id, group_id))

    monkeypatch.setattr(fds, "request_with_retry", fake_request)
    monkeypatch.setattr(fds, "emit_stage_update_event", fake_emit)
    monkeypatch.setattr(fds, "dispatch", fake_dispatch)
    monkeypatch.setattr(fds, "emit_audit_log", fake_audit_log)

    with pytest.raises(requests.HTTPError):
        dispatch(
            "finance.decision.request",
            {"budget": 100, "max_options": 3},
            user_id="alice",
        )

    assert len(calls) == 3
    assert emitted == [("finance.decision.result", "error", "alice", None)]
    assert "finance.decision.result" not in dispatched
    assert any(a[1] == "persistence" and a[2] == "error" and "boom" in a[3] and "nodes" in a[4] for a in audit_logs)
    assert any(a[1] == "workflow" and a[2] == "error" for a in audit_logs)


def test_financial_decision_support_missing_fields(monkeypatch):
    calls = []

    def fake_request(method, url, timeout, **kwargs):
        calls.append((method, url, kwargs))
        return DummyResponse({})

    emitted = []

    def fake_emit(name, stage, user_id=None, group_id=None, **_):
        emitted.append((name, stage, user_id, group_id))

    dispatched = []

    def fake_dispatch(event, payload, user_id, group_id=None):
        dispatched.append(event)

    monkeypatch.setattr(fds, "request_with_retry", fake_request)
    monkeypatch.setattr(fds, "emit_stage_update_event", fake_emit)
    monkeypatch.setattr(fds, "dispatch", fake_dispatch)
    monkeypatch.setattr(fds, "emit_audit_log", lambda *a, **k: None)

    with pytest.raises(ValueError):
        dispatch("finance.decision.request", {}, user_id="alice")

    assert calls == []
    assert emitted == [("finance.decision.result", "error", "alice", None)]
    assert "finance.decision.result" not in dispatched
