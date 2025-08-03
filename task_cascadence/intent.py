"""Intent resolution using an LLM with optional UME context."""

from __future__ import annotations

from dataclasses import dataclass
from html import escape
import re
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


SENSITIVE_PATTERNS = [
    re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),
    re.compile(r"\b(?:\d[ -]*?){13,16}\b"),
]


def sanitize_input(text: str) -> str:
    """Return ``text`` with dangerous content stripped and sensitive data redacted."""

    # Remove script tags completely
    sanitized = re.sub(r"<script.*?>.*?</script>", "", text, flags=re.IGNORECASE | re.DOTALL)
    # Escape angle brackets and other HTML special chars
    sanitized = escape(sanitized)
    # Remove characters often used for command injection
    sanitized = re.sub(r"[`$]", "", sanitized)
    sanitized = re.sub(r"rm\s+-rf\s+/?", "", sanitized, flags=re.IGNORECASE)
    # Redact sensitive patterns like emails or credit card numbers
    for pattern in SENSITIVE_PATTERNS:
        sanitized = pattern.sub("[REDACTED]", sanitized)
    return sanitized


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

    ctx = [sanitize_input(c) for c in (context or [])]
    message = sanitize_input(message)
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
