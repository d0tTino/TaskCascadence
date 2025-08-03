from task_cascadence.workflows import dispatch
from task_cascadence.workflows import financial_decision_support as fds


class DummyResponse:
    def __init__(self, data):
        self._data = data

    def json(self):
        return self._data


def test_financial_decision_support(monkeypatch):
    calls = []

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

    def fake_dispatch(event, payload, user_id=None, group_id=None):
        dispatched.append((event, payload, user_id, group_id))

    monkeypatch.setattr(fds, "request_with_retry", fake_request)
    monkeypatch.setattr(fds, "emit_stage_update_event", fake_emit)
    monkeypatch.setattr(fds, "dispatch", fake_dispatch)

    result = dispatch(
        "finance.decision.request",
        {"explain": True, "group_id": "g1", "budget": 100, "max_options": 3},
        user_id="alice",
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
    assert emitted["event"] == ("finance.decision.result", "completed", "alice", "g1")
    assert dispatched[0][0] == "finance.decision.result"
    assert dispatched[0][1]["summary"]["cost_of_deviation"] == 50
    assert dispatched[0][2] == "alice"
    assert dispatched[0][3] == "g1"
    assert dispatched[1][0] == "finance.explain.request"
    assert dispatched[1][1]["actions"][0]["id"] == "act1"
    assert result["analysis"] == "da1"
    assert result["summary"]["cost_of_deviation"] == 50
