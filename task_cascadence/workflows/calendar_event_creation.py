from __future__ import annotations

import asyncio
import threading
from datetime import datetime, timedelta
from typing import Any, Dict, List

from . import subscribe

from ..http_utils import request_with_retry
from .. import research
from ..ume import emit_stage_update_event, emit_task_note
from ..ume.models import TaskNote


def _has_permission(
    user_id: str,
    *,
    ume_base: str = "http://ume",
    group_id: str | None = None,
    invitee: str | None = None,

) -> bool:
    """Return ``True`` if ``user_id`` may create calendar events."""

    url = f"{ume_base.rstrip('/')}/v1/permissions/calendar:create"
    params: Dict[str, Any] = {"user_id": user_id}
    if group_id is not None:
        params["group_id"] = group_id
    if invitee is not None:
        params["invitee"] = invitee

    resp = request_with_retry("GET", url, params=params, timeout=5)
    data = resp.json()
    return bool(data.get("allowed"))


@subscribe("calendar.event.create_request")
def create_calendar_event(
    payload: Dict[str, Any], *, user_id: str, base_url: str = "http://localhost", ume_base: str = "http://ume"

) -> Dict[str, Any]:
    """Persist calendar events after validation and permission checks."""

    for field in ("title", "start_time"):
        if field not in payload or not payload[field]:
            raise ValueError(f"missing required field: {field}")

    group_id = payload.get("group_id")

    travel_task: asyncio.Task | None = None
    travel_thread: threading.Thread | None = None
    if payload.get("location"):
        loop = asyncio.new_event_loop()

        async def _inner() -> None:
            nonlocal travel_task
            travel_task = asyncio.create_task(
                research.async_gather(
                    f"travel time to {payload['location']}",
                    user_id=user_id,
                    group_id=group_id,
                )
            )
            await travel_task

        def _run() -> None:  # pragma: no cover - executed in thread
            asyncio.set_event_loop(loop)
            loop.run_until_complete(_inner())
            loop.close()

        travel_thread = threading.Thread(target=_run)
        travel_thread.start()

    if not _has_permission(user_id, ume_base=ume_base):
        raise PermissionError("user lacks calendar:create permission")

    if group_id and not _has_permission(user_id, ume_base=ume_base, group_id=group_id):
        raise PermissionError("user lacks group permission")

    invitees: List[str] = payload.get("invitees", []) or []
    for invitee in invitees:
        if not _has_permission(user_id, ume_base=ume_base, invitee=invitee):
            raise PermissionError(f"user lacks permission to invite {invitee}")

    if travel_thread:
        travel_thread.join()

    event_data = dict(payload)
    event_data["user_id"] = user_id
    if group_id:
        event_data["group_id"] = group_id

    related_event: Dict[str, Any] | None = None
    travel_info: Dict[str, Any] | None = None
    if travel_task is not None:
        try:
            travel_info = travel_task.result()
            research.gather(
                f"travel time to {payload['location']}",
                user_id=user_id,
                group_id=group_id,
            )
            event_data["travel_time"] = travel_info
            start_dt = datetime.fromisoformat(
                payload["start_time"].replace("Z", "+00:00")
            )
            duration = travel_info.get("duration")
            leave_dt = start_dt
            if isinstance(duration, str) and duration.endswith("m"):
                leave_dt = start_dt - timedelta(minutes=int(duration[:-1]))
            related_event = {
                "title": f"Leave by {payload['title']}",
                "start_time": leave_dt.isoformat(),
                "user_id": user_id,
            }
            if group_id:
                related_event["group_id"] = group_id
        except Exception:
            travel_info = None

    note_text = "No travel details"
    if travel_info:
        duration = travel_info.get("duration")
        note_text = f"Travel time to {payload['location']}: {duration}"
    note = TaskNote(note=note_text)

    url = f"{base_url.rstrip('/')}/v1/calendar/events"
    resp = request_with_retry("POST", url, json=event_data, timeout=5)
    main_event = resp.json()
    main_id = main_event.get("id")

    edge_url = f"{base_url.rstrip('/')}/v1/calendar/edges"

    related_id = None
    if related_event is not None:
        rel_resp = request_with_retry("POST", url, json=related_event, timeout=5)
        related_event = rel_resp.json()
        related_id = related_event.get("id")
        edge_payload = {
            "src": related_id,
            "dst": main_id,
            "type": "RELATES_TO",
            "user_id": user_id,
        }
        if group_id:
            edge_payload["group_id"] = group_id
        request_with_retry("POST", edge_url, json=edge_payload, timeout=5)

    for invitee in invitees:
        edge_payload = {
            "src": main_id,
            "dst": invitee,
            "type": "INVITED",
            "user_id": user_id,
        }
        if group_id:
            edge_payload["group_id"] = group_id
        request_with_retry("POST", edge_url, json=edge_payload, timeout=5)

    for layer in payload.get("layers", []) or []:
        edge_payload = {
            "src": main_id,
            "dst": layer,
            "type": "LAYER",
            "user_id": user_id,
        }
        if group_id:
            edge_payload["group_id"] = group_id
        request_with_retry("POST", edge_url, json=edge_payload, timeout=5)

    emit_stage_update_event(
        "calendar.event.created",
        "created",
        user_id=user_id,
        group_id=group_id,
        event_id=main_id,

    )
    note = TaskNote(note=f"Created event {payload['title']}")
    emit_task_note(note, user_id=user_id, group_id=group_id)

    result: Dict[str, Any] = {"event_id": main_id}
    if related_id:
        result["related_event_id"] = related_id

    dispatch("calendar.event.created", result, user_id=user_id, group_id=group_id)
    return result
