from __future__ import annotations

from typing import Any

from .stage_store import StageStore
from .ume import _hash_user_id


def recover_partial(
    task_name: str,
    stage: str,
    *,
    user_id: str | None = None,
    group_id: str | None = None,
) -> Any | None:
    """Return the most recent partial payload for *task_name* and *stage*.

    Parameters
    ----------
    task_name:
        Name of the task whose audit log should be queried.
    stage:
        Stage name to look up within the audit events.
    user_id:
        Optional user identifier. When provided it is hashed in the same way as
        :func:`task_cascadence.ume.emit_audit_log` so that per-user isolation is
        respected.
    group_id:
        Optional group identifier to further scope the lookup.

    Returns
    -------
    Any | None
        The stored ``partial`` payload if one exists, otherwise ``None``.
    """

    store = StageStore()
    user_hash = _hash_user_id(user_id) if user_id is not None else None
    events = store.get_events(
        task_name,
        user_hash=user_hash,
        group_id=group_id,
        category="audit",
    )
    for event in reversed(events):
        if event.get("stage") == stage and "partial" in event:
            return event["partial"]
    return None
