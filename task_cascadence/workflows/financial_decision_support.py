from __future__ import annotations

from typing import Any, Dict, List

from . import subscribe, dispatch
from ..http_utils import request_with_retry
from ..ume import emit_stage_update_event, emit_audit_log


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
    emit_audit_log(
        "finance.decision",
        "workflow",
        "started",
        user_id=user_id,
        group_id=group_id,
    )

    payload_group_id = payload.get("group_id")
    if payload_group_id is not None and payload_group_id != group_id:
        emit_stage_update_event(
            "finance.decision.result", "error", user_id=user_id, group_id=group_id
        )
        raise ValueError("Payload group_id does not match caller group_id")
    time_horizon = payload.get("time_horizon")

    required_fields = {"budget", "max_options"}
    missing = [f for f in required_fields if f not in payload]
    if missing:
        emit_stage_update_event(
            "finance.decision.result", "error", user_id=user_id, group_id=group_id
        )
        raise ValueError(f"Missing required fields: {', '.join(missing)}")

    try:
        url = f"{ume_base.rstrip('/')}/v1/nodes"
        params: Dict[str, Any] = {
            "types": "FinancialAccount,FinancialGoal,DecisionAnalysis",
            "user_id": user_id,
        }
        if group_id is not None:
            params["group_id"] = group_id
        if time_horizon is not None:
            params["time_horizon"] = time_horizon
        resp = request_with_retry("GET", url, params=params, timeout=5)
        data = resp.json()
        nodes: List[Dict[str, Any]] = data.get("nodes", [])

        accounts = [n for n in nodes if n.get("type") == "FinancialAccount"]
        goals = [n for n in nodes if n.get("type") == "FinancialGoal"]
        analyses = [n for n in nodes if n.get("type") == "DecisionAnalysis"]

        total_balance = sum(a.get("balance", 0) for a in accounts)

        engine_payload = {
            "balance": total_balance,
            "goals": goals,
            "analyses": analyses,
            "user_id": user_id,
        }
        if group_id is not None:
            engine_payload["group_id"] = group_id

        if "budget" in payload:
            engine_payload["budget"] = payload["budget"]
        if "max_options" in payload:
            engine_payload["max_options"] = payload["max_options"]

        try:
            eng_resp = request_with_retry(
                "POST",
                f"{engine_base.rstrip('/')}/v1/simulations/debt",
                json=engine_payload,
                timeout=5,
            )
        except Exception as exc:
            emit_audit_log(
                "finance.decision",
                "engine",
                "error",
                reason=str(exc),
                output=str(engine_payload),
                user_id=user_id,
                group_id=group_id,
            )
            raise
        eng_result = eng_resp.json()

        analysis_id = eng_result.get("id", "analysis")
        actions: List[Dict[str, Any]] = eng_result.get("actions", [])
        analysis_node: Dict[str, Any] = {
            "id": analysis_id,
            "type": "DecisionAnalysis",
            "metrics": {"cost_of_deviation": eng_result.get("cost_of_deviation", 0)},
            "user_id": user_id,
        }
        if group_id is not None:
            analysis_node["group_id"] = group_id
        if time_horizon is not None:
            analysis_node["metadata"] = {"time_horizon": time_horizon}
        nodes_to_persist = [analysis_node]
        edges = []

        for act in actions:
            act_id = act.get("id")
            node = {
                "id": act_id,
                "type": "ProposedAction",
                "metrics": {"cost_of_deviation": act.get("cost_of_deviation")},
                "user_id": user_id,
            }
            if group_id is not None:
                node["group_id"] = group_id
            nodes_to_persist.append(node)
            edge = {"src": analysis_id, "dst": act_id, "type": "CONSIDERS", "user_id": user_id}
            if group_id is not None:
                edge["group_id"] = group_id
            edges.append(edge)
            for account in accounts:
                owned_edge = {
                    "src": act_id,
                    "dst": account.get("id"),
                    "type": "OWNED_BY",
                    "user_id": user_id,
                }
                if group_id is not None:
                    owned_edge["group_id"] = group_id
                edges.append(owned_edge)

        try:
            request_with_retry(
                "POST",
                url,
                json={"nodes": nodes_to_persist, "edges": edges},
                timeout=5,
            )
        except Exception as exc:
            emit_audit_log(
                "finance.decision",
                "persistence",
                "error",
                reason=str(exc),
                output=str({"nodes": nodes_to_persist, "edges": edges}),
                user_id=user_id,
                group_id=group_id,
            )
            raise

        summary = {"cost_of_deviation": eng_result.get("cost_of_deviation", 0)}
        context = {"analysis": analysis_id, "summary": summary}

        dispatch("finance.decision.result", context, user_id=user_id, group_id=group_id)
        if payload.get("explain"):
            dispatch(
                "finance.explain.request",
                {**context, "actions": actions},

                user_id=user_id,
                group_id=group_id,
            )

        emit_stage_update_event(
            "finance.decision.result", "completed", user_id=user_id, group_id=group_id
        )
        emit_audit_log(
            "finance.decision",
            "workflow",
            "completed",
            user_id=user_id,
            group_id=group_id,
        )

        return {**context, "actions": actions}
    except Exception as exc:
        emit_audit_log(
            "finance.decision",
            "workflow",
            "error",
            reason=str(exc),
            user_id=user_id,
            group_id=group_id,
        )
        emit_stage_update_event(
            "finance.decision.result", "error", user_id=user_id, group_id=group_id
        )
        raise

