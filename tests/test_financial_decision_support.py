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

    explains = {}

    def fake_dispatch(event, payload, user_id=None, group_id=None):
        explains["called"] = (event, payload, user_id, group_id)

    monkeypatch.setattr(fds, "request_with_retry", fake_request)
    monkeypatch.setattr(fds, "emit_stage_update_event", fake_emit)
    monkeypatch.setattr(fds, "dispatch", fake_dispatch)

    result = fds.financial_decision_support(
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
    assert any(
        n["type"] == "DecisionAnalysis" and n["metrics"]["cost_of_deviation"] == 50
        for n in persist["nodes"]
    )
    assert any(
        n["type"] == "ProposedAction" and n["metrics"]["cost_of_deviation"] == 20
        for n in persist["nodes"]
    )
    assert {"src": "da1", "dst": "act1", "type": "CONSIDERS"} in persist["edges"]
    assert {"src": "act1", "dst": "acct1", "type": "OWNED_BY"} in persist["edges"]
    assert emitted["event"] == ("finance.decision.result", "completed", "alice", "g1")
    assert explains["called"][0] == "finance.explain.request"
    assert explains["called"][3] == "g1"
    assert result["analysis"] == "da1"
    assert result["summary"]["cost_of_deviation"] == 50
