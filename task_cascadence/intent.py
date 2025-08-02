"""Intent resolution using an LLM with optional UME context."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .research import gather


@dataclass
class IntentResult:
    """Represents the inferred task intent."""

    task: Optional[str]
    arguments: Dict[str, Any]
    confidence: float
    requires_clarification: bool = False


CLARIFICATION_THRESHOLD = 0.7


def _build_prompt(message: str, context: List[str]) -> str:
    """Combine *message* and *context* into a single prompt for the LLM."""

    parts = [
        "You are an intent classification service."
        " Use the provided context to resolve references in the user message.",
    ]
    if context:
        parts.append("Context:\n" + "\n".join(context))
    parts.append(f"User: {message}")
    parts.append(
        "Respond with JSON containing keys 'task', 'arguments' and 'confidence'"
    )
    return "\n\n".join(parts)


def resolve_intent(
    message: str, context: Optional[List[str]] = None, *, threshold: float = CLARIFICATION_THRESHOLD
) -> IntentResult:
    """Return the :class:`IntentResult` for *message* using *context* for disambiguation."""

    ctx = context or []
    prompt = _build_prompt(message, ctx)
    result = gather(prompt)
    task: Optional[str] = None
    args: Dict[str, Any] = {}
    confidence = 0.0
    if isinstance(result, dict):
        task = result.get("task")
        args = result.get("arguments") or result.get("args") or {}
        confidence = float(result.get("confidence", 0.0))
    intent = IntentResult(task=task, arguments=args, confidence=confidence)
    if confidence < threshold:
        intent.requires_clarification = True
    return intent
