from __future__ import annotations

from typing import Any, Callable, Dict

_registry: Dict[str, Callable[..., Any]] = {}


def subscribe(event: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Decorator to register a workflow for *event*."""

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        _registry[event] = func
        return func

    return decorator


def dispatch(event: str, *args: Any, **kwargs: Any) -> Any:
    """Dispatch *event* to the registered workflow."""

    handler = _registry.get(event)
    if not handler:
        raise ValueError(f"No workflow registered for {event}")
    return handler(*args, **kwargs)


# Import built-in workflows so they register themselves
from . import calendar_event_creation  # noqa: F401,E402
