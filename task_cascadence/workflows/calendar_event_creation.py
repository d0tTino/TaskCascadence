from __future__ import annotations

from typing import Any, Dict, List
from datetime import datetime, timedelta
import uuid

from . import subscribe
from ..http_utils import request_with_retry
from .. import research
from ..ume import emit_stage_update_event


def _has_permission(
    user_id: str,
    group_id: str | None = None,
    *,
    ume_base: str = "http://ume",
) -> bool:
    """Return ``True`` if ``user_id`` may create calendar events."""

    url = f"{ume_base.rstrip('/')}/v1/permissions/calendar:create"
    params = {"user_id": user_id}
    if group_id is not None:
        params["group_id"] = group_id
    resp = request_with_retry("GET", url, params=params, timeout=5)
    data = resp.json()
    return bool(data.get("allowed"))


@subscribe("calendar.event.request")
def create_calendar_event(
    payload: Dict[str, Any],
    *,
    user_id: str,
    group_id: str | None = None,
    base_url: str = "http://localhost",
    ume_base: str = "http://ume",
) -> Dict[str, Any]:
    """Persist calendar events after validation and permission checks.

    A "Leave by" event will be created when travel time information is
    available.  The travel event is linked to the main event via a
    ``RELATES_TO`` edge.
    """

    for field in ("title", "start", "end", "location"):
        if field not in payload:
            raise ValueError(f"missing required field: {field}")
    if not _has_permission(user_id, group_id, ume_base=ume_base):
        raise PermissionError("user lacks calendar:create permission")

    main_id = f"evt-{uuid.uuid4().hex}"
    nodes: List[Dict[str, Any]] = [dict(id=main_id, **payload)]
    edges: List[Dict[str, Any]] = []

    try:
        travel = research.gather(
            f"travel time to {payload['location']}",
            user_id=user_id,
            group_id=group_id,
        )
        nodes[0]["travel_time"] = travel

        dur = travel.get("duration")
        if isinstance(dur, str) and dur.endswith("m"):
            minutes = int(dur[:-1])
            start = _parse_time(payload["start"]) - timedelta(minutes=minutes)
            leave_id = f"evt-{uuid.uuid4().hex}"
            nodes.append(
                {
                    "id": leave_id,
                    "title": f"Leave by {start.strftime('%H:%M')}",
                    "start": _format_time(start),
                    "end": payload["start"],
                }
            )
            edges.append(
                {
                    "src": leave_id,
                    "dst": main_id,
                    "type": "RELATES_TO",
                    "data": {"reason": "travel"},
                }
            )
    except RuntimeError:
        pass

    url = f"{base_url.rstrip('/')}/v1/calendar/events"
    resp = request_with_retry(
        "POST", url, json={"nodes": nodes, "edges": edges}, timeout=5
    )
    emit_stage_update_event(
        "calendar.event.created", "created", user_id=user_id, group_id=group_id
    )
    return resp.json()


def _parse_time(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def _format_time(dt: datetime) -> str:
    return dt.replace(tzinfo=None).isoformat(timespec="seconds") + "Z"
