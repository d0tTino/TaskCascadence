from __future__ import annotations


from datetime import datetime, timedelta
from typing import Any, Dict, List
import asyncio
import contextlib

from . import dispatch, subscribe

from ..http_utils import request_with_retry
from .. import research
from ..ume import emit_stage_update_event, emit_task_note, emit_audit_log
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


def validate_payload(
    payload: Dict[str, Any],
    *,
    user_id: str,
    group_id: str | None = None,
) -> None:
    """Ensure required fields are present and ``group_id`` matches."""

    for field in ("title", "start_time"):
        if field not in payload or not payload[field]:
            emit_audit_log(
                "calendar.event.create",
                "workflow",
                "error",
                reason=f"missing required field: {field}",
                user_id=user_id,
                group_id=group_id,
            )
            raise ValueError(f"missing required field: {field}")

    payload_group_id = payload.get("group_id")
    if payload_group_id is not None and payload_group_id != group_id:
        emit_audit_log(
            "calendar.event.create",
            "workflow",
            "error",
            reason="group_id mismatch",
            user_id=user_id,
            group_id=group_id,
        )
        raise ValueError("group_id mismatch")


async def gather_travel_info(
    payload: Dict[str, Any],
    *,
    user_id: str,
    group_id: str | None = None,
) -> Dict[str, Any] | None:
    """Return travel research info if ``location`` is provided."""

    if not payload.get("location"):
        return None
    query = f"travel time to {payload['location']}"
    try:
        info = await research.async_gather(
            query, user_id=user_id, group_id=group_id
        )
        emit_audit_log(
            "calendar.event.create",
            "research",
            "success",
            user_id=user_id,
            group_id=group_id,
        )
        return info
    except Exception as exc:  # pragma: no cover - network failures
        emit_audit_log(
            "calendar.event.create",
            "research",
            "error",
            reason=str(exc),
            user_id=user_id,
            group_id=group_id,
        )
        return None


async def check_permissions(
    user_id: str,
    invitees: List[str],
    *,
    ume_base: str = "http://ume",
    group_id: str | None = None,
) -> None:
    """Validate that ``user_id`` may create events and invite others."""

    try:
        allowed = await asyncio.to_thread(
            _has_permission, user_id, ume_base=ume_base, group_id=group_id
        )
        if not allowed:
            raise ValueError("user lacks calendar:create permission")
    except Exception as exc:  # pragma: no cover - network failures
        emit_audit_log(
            "calendar.event.create",
            "permission",
            "error",
            reason=str(exc),
            user_id=user_id,
            group_id=group_id,
        )
        raise

    async def _check_invitee(invitee: str) -> None:
        try:
            allowed = await asyncio.to_thread(
                _has_permission,
                user_id,
                ume_base=ume_base,
                group_id=group_id,
                invitee=invitee,
            )
            if not allowed:
                raise ValueError(
                    f"user lacks permission to invite {invitee}"
                )
        except Exception as exc:  # pragma: no cover - network failures
            emit_audit_log(
                "calendar.event.create",
                "permission",
                "error",
                reason=str(exc),
                user_id=user_id,
                group_id=group_id,
            )
            raise

    await asyncio.gather(*(_check_invitee(i) for i in invitees))


def _post_calendar_event(
    event_data: Dict[str, Any],
    *,
    ume_base: str,
    user_id: str,
    group_id: str | None = None,
) -> str | None:
    """Persist a calendar event and return its identifier."""

    url = f"{ume_base.rstrip('/')}/v1/calendar/events"
    try:
        resp = request_with_retry("POST", url, json=event_data, timeout=5)
    except Exception as exc:
        emit_audit_log(
            "calendar.event.create",
            "persistence",
            "error",
            reason=str(exc),
            user_id=user_id,
            group_id=group_id,
        )
        raise
    else:
        emit_audit_log(
            "calendar.event.create",
            "persistence",
            "success",
            user_id=user_id,
            group_id=group_id,
        )
        event = resp.json()
        return event.get("id")


def _post_calendar_edge(
    src: str | None,
    dst: str | None,
    edge_type: str,
    *,
    ume_base: str,
    user_id: str,
    group_id: str | None = None,
) -> None:
    """Persist a calendar edge with audit logging."""

    if src is None or dst is None:
        return

    edge_url = f"{ume_base.rstrip('/')}/v1/calendar/edges"
    edge_payload = {
        "src": src,
        "dst": dst,
        "type": edge_type,
        "user_id": user_id,
    }
    if group_id:
        edge_payload["group_id"] = group_id
    try:
        request_with_retry("POST", edge_url, json=edge_payload, timeout=5)
    except Exception as exc:
        emit_audit_log(
            "calendar.event.create",
            "persistence",
            "error",
            reason=str(exc),
            user_id=user_id,
            group_id=group_id,
        )
        raise
    else:
        emit_audit_log(
            "calendar.event.create",
            "persistence",
            "success",
            user_id=user_id,
            group_id=group_id,
        )


def persist_events(
    event_data: Dict[str, Any],
    invitees: List[str],
    layers: List[str],
    *,
    ume_base: str,
    user_id: str,
    group_id: str | None = None,
    related_event: Dict[str, Any] | None = None,
) -> tuple[str | None, str | None]:
    """Persist the calendar event and related edges."""

    main_id = _post_calendar_event(
        event_data,
        ume_base=ume_base,
        user_id=user_id,
        group_id=group_id,
    )

    related_id = None
    edges_to_create: List[tuple[str | None, str | None, str]] = []
    if related_event is not None:
        related_id = _post_calendar_event(
            related_event,
            ume_base=ume_base,
            user_id=user_id,
            group_id=group_id,
        )
        edges_to_create.append((related_id, main_id, "RELATES_TO"))

    edges_to_create.extend(
        (main_id, target, edge_type)
        for targets, edge_type in ((invitees, "INVITED"), (layers, "LAYER"))
        for target in targets
    )

    for src, dst, edge_type in edges_to_create:
        _post_calendar_edge(
            src,
            dst,
            edge_type,
            ume_base=ume_base,
            user_id=user_id,
            group_id=group_id,
        )

    return main_id, related_id


def persist_related_event(
    main_event_id: str | None,
    related_event: Dict[str, Any],
    *,
    ume_base: str,
    user_id: str,
    group_id: str | None = None,
) -> str | None:
    """Persist a follow-up event and relate it to the main event."""

    related_id = _post_calendar_event(
        related_event,
        ume_base=ume_base,
        user_id=user_id,
        group_id=group_id,
    )
    _post_calendar_edge(
        related_id,
        main_event_id,
        "RELATES_TO",
        ume_base=ume_base,
        user_id=user_id,
        group_id=group_id,
    )
    return related_id


@subscribe("calendar.event.create_request")
async def create_calendar_event(
    payload: Dict[str, Any],
    *,
    user_id: str,
    group_id: str | None = None,
    ume_base: str = "http://ume",
) -> Dict[str, Any]:
    """Persist calendar events after validation and permission checks."""
    emit_audit_log(
        "calendar.event.create",
        "workflow",
        "started",
        user_id=user_id,
        group_id=group_id,
    )

    validate_payload(payload, user_id=user_id, group_id=group_id)

    travel_task = asyncio.create_task(
        gather_travel_info(payload, user_id=user_id, group_id=group_id)
    )
    participant_lists: Dict[str, List[str]] = {
        key: payload.get(key, []) or []
        for key in ("invitees", "layers")
    }
    invitees: List[str] = participant_lists["invitees"]
    layers: List[str] = participant_lists["layers"]
    perm_task = asyncio.create_task(
        check_permissions(
            user_id,
            invitees,
            ume_base=ume_base,
            group_id=group_id,
        )
    )
    try:
        await perm_task
        emit_audit_log(
            "calendar.event.create",
            "permission",
            "success",
            user_id=user_id,
            group_id=group_id,
        )
    except Exception:
        travel_task.cancel()
        with contextlib.suppress(Exception):
            await travel_task
        raise

    event_data = dict(payload)
    event_data["user_id"] = user_id
    if group_id:
        event_data["group_id"] = group_id

    main_id, _ = persist_events(
        event_data,
        invitees,
        layers,
        ume_base=ume_base,
        user_id=user_id,
        group_id=group_id,
    )

    travel_info = await travel_task
    emit_audit_log(
        "calendar.event.create",
        "research",
        "completed",
        user_id=user_id,
        group_id=group_id,
    )

    related_id = None
    if travel_info is not None:
        try:
            start_dt = datetime.fromisoformat(
                payload["start_time"].replace("Z", "+00:00")
            )
        except Exception:
            start_dt = datetime.fromisoformat(payload["start_time"])
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
        related_id = persist_related_event(
            main_id,
            related_event,
            ume_base=ume_base,
            user_id=user_id,
            group_id=group_id,
        )

    note_text = "No travel details"
    if travel_info:
        duration = travel_info.get("duration")
        note_text = f"Travel time to {payload['location']}: {duration}"

    note = TaskNote(note=note_text)

    result: Dict[str, Any] = {"event_id": main_id}
    if related_id:
        result["related_event_id"] = related_id

    try:
        emit_stage_update_event(
            "calendar.event.created",
            "created",
            user_id=user_id,
            group_id=group_id,
            event_id=main_id,

        )
    except Exception as exc:  # pragma: no cover - network failures
        emit_audit_log(
            "calendar.event.create",
            "emit_stage_update_event",
            "error",
            reason=str(exc),
            user_id=user_id,
            group_id=group_id,
        )
    try:
        emit_task_note(note, user_id=user_id, group_id=group_id)
    except Exception as exc:  # pragma: no cover - network failures
        emit_audit_log(
            "calendar.event.create",
            "emit_task_note",
            "error",
            reason=str(exc),
            user_id=user_id,
            group_id=group_id,
        )

    dispatch("calendar.event.created", result, user_id=user_id, group_id=group_id)
    emit_audit_log(
        "calendar.event.create",
        "workflow",
        "completed",
        user_id=user_id,
        group_id=group_id,
    )
    return result
