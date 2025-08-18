from __future__ import annotations

from threading import Lock
from typing import Any, Callable, Dict
import inspect

from ..async_utils import run_coroutine

_registry: Dict[str, Callable[..., Any]] = {}
_registry_lock = Lock()


def subscribe(event: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator to register a workflow for *event*."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        with _registry_lock:
            _registry[event] = func
        return func

    return decorator


def dispatch(
    event: str,
    *args: Any,
    user_id: str | None = None,
    group_id: str | None = None,
    **kwargs: Any,
) -> Any:
    """Dispatch *event* to the registered workflow."""

    if user_id is None:
        raise ValueError("user_id is required")
    with _registry_lock:
        handler = _registry.get(event)
    if not handler:
        raise ValueError(f"No workflow registered for {event}")
    if group_id is None:
        result = handler(*args, user_id=user_id, **kwargs)
    else:
        result = handler(*args, user_id=user_id, group_id=group_id, **kwargs)
    if inspect.isawaitable(result):
        return run_coroutine(result)
    return result


# Import built-in workflows so they register themselves
from . import calendar_event_creation  # noqa: F401,E402
from . import financial_decision_support  # noqa: F401,E402
