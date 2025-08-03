from __future__ import annotations

from typing import Any, Dict

from . import subscribe
from ..http_utils import request_with_retry
from .. import research
from ..ume import emit_stage_update_event


def _has_permission(user_id: str, *, ume_base: str = "http://ume") -> bool:
    """Return ``True`` if ``user_id`` may create calendar events."""

    url = f"{ume_base.rstrip('/')}/v1/permissions/calendar:create"
    resp = request_with_retry("GET", url, params={"user_id": user_id}, timeout=5)
    data = resp.json()
    return bool(data.get("allowed"))


@subscribe("calendar.event.create")
def create_calendar_event(
    payload: Dict[str, Any], *, user_id: str, base_url: str = "http://localhost"
) -> Dict[str, Any]:
    """Persist a calendar event after validation and permission checks."""

    for field in ("title", "start", "end"):
        if field not in payload:
            raise ValueError(f"missing required field: {field}")
    if not _has_permission(user_id):
        raise PermissionError("user lacks calendar:create permission")

    event_data = dict(payload)
    if payload.get("location"):
        try:
            event_data["travel_time"] = research.gather(
                f"travel time to {payload['location']}", user_id=user_id
            )
        except RuntimeError:
            pass

    url = f"{base_url.rstrip('/')}/v1/calendar/events"
    resp = request_with_retry("POST", url, json=event_data, timeout=5)
    emit_stage_update_event("calendar.event.created", "created", user_id=user_id)
    return resp.json()
