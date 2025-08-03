from __future__ import annotations

from typing import Any, Dict, List

from . import subscribe, dispatch
from ..http_utils import request_with_retry
from ..ume import emit_stage_update_event


@subscribe("finance.decision.request")
def financial_decision_support(
    payload: Dict[str, Any],
    *,
    user_id: str,
    group_id: str | None = None,
    ume_base: str = "http://ume",
    engine_base: str = "http://finance-engine",
) -> Dict[str, Any]:
    """Aggregate financial data, run a debt simulation, and persist results."""

    url = f"{ume_base.rstrip('/')}/v1/nodes"
    params: Dict[str, Any] = {
        "types": "FinancialAccount,FinancialGoal,DecisionAnalysis",
        "user_id": user_id,
    }
    if group_id is not None:
        params["group_id"] = group_id
    resp = request_with_retry("GET", url, params=params, timeout=5)
    data = resp.json()
    nodes: List[Dict[str, Any]] = data.get("nodes", [])

    accounts = [n for n in nodes if n.get("type") == "FinancialAccount"]
    goals = [n for n in nodes if n.get("type") == "FinancialGoal"]
    analyses = [n for n in nodes if n.get("type") == "DecisionAnalysis"]

    total_balance = sum(a.get("balance", 0) for a in accounts)

    engine_payload: Dict[str, Any] = {
        "balance": total_balance,
        "goals": goals,
        "analyses": analyses,
    }
    if "budget" in payload:
        engine_payload["budget"] = payload["budget"]
    if "max_options" in payload:
        engine_payload["max_options"] = payload["max_options"]
    eng_resp = request_with_retry(
        "POST", f"{engine_base.rstrip('/')}/v1/simulations/debt", json=engine_payload, timeout=5
    )
    eng_result = eng_resp.json()

    analysis_id = eng_result.get("id", "analysis")
    actions: List[Dict[str, Any]] = eng_result.get("actions", [])
    nodes_to_persist = [
        {
            "id": analysis_id,
            "type": "DecisionAnalysis",
            "metrics": {"cost_of_deviation": eng_result.get("cost_of_deviation", 0)},
        }
    ]
    edges = []

    for act in actions:
        act_id = act.get("id")
        nodes_to_persist.append(
            {
                "id": act_id,
                "type": "ProposedAction",
                "metrics": {"cost_of_deviation": act.get("cost_of_deviation")},
            }
        )
        edges.append({"src": analysis_id, "dst": act_id, "type": "CONSIDERS"})
        if accounts:
            edges.append({"src": act_id, "dst": accounts[0].get("id"), "type": "OWNED_BY"})

    request_with_retry("POST", url, json={"nodes": nodes_to_persist, "edges": edges}, timeout=5)

    if payload.get("explain"):
        dispatch(
            "finance.explain.request",
            {"analysis": analysis_id, "actions": actions},
            user_id=user_id,
            group_id=group_id,
        )

    emit_stage_update_event(
        "finance.decision.result", "completed", user_id=user_id, group_id=group_id
    )

    return {
        "analysis": analysis_id,
        "summary": {"cost_of_deviation": eng_result.get("cost_of_deviation", 0)},
        "actions": actions,
    }
