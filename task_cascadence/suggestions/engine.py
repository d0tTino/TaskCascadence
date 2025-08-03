from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Dict, List
import inspect
from uuid import uuid4

from ..scheduler import get_default_scheduler
from ..research import gather
from ..ume import (
    emit_acceptance_event,
    is_private_event,
    detect_event_patterns,
    record_suggestion_decision,
)
from ..config import load_config


@dataclass
class Suggestion:
    """Representation of a generated suggestion."""

    id: str
    title: str
    description: str
    confidence: float
    related_entities: List[str] = field(default_factory=list)
    task_name: str | None = None
    context: Dict[str, Any] = field(default_factory=dict)
    state: str = "pending"  # pending, snoozed, dismissed, accepted


class SuggestionEngine:
    """Generate and manage task suggestions."""

    def __init__(self, interval: float = 60.0) -> None:
        self.interval = interval
        self._suggestions: Dict[str, Suggestion] = {}
        self._task: asyncio.Task | None = None
        self._user_id: str | None = None
        self._group_id: str | None = None

    async def _query_ume(self) -> List[dict]:
        """Return user-event patterns from UME."""

        return detect_event_patterns()

    async def generate(
        self, user_id: str | None = None, group_id: str | None = None
    ) -> None:
        """Query UME and create suggestions once."""
        if user_id is None:
            user_id = self._user_id
        if user_id is None:
            raise RuntimeError("user_id required to generate suggestions")
        if group_id is None:
            group_id = self._group_id

        cfg = load_config().get("suggestions", {})
        if not cfg.get("enabled", True):
            return
        disabled_categories = set(cfg.get("categories", []))

        patterns = await self._query_ume()
        for pattern in patterns:
            if is_private_event(pattern):
                continue
            pattern_categories: set[str] = set()
            category = pattern.get("category")
            if category:
                if isinstance(category, (list, tuple, set)):
                    pattern_categories.update(category)
                else:
                    pattern_categories.add(category)
            categories = pattern.get("categories")
            if categories:
                if isinstance(categories, str):
                    pattern_categories.add(categories)
                else:
                    pattern_categories.update(categories)
            if disabled_categories & pattern_categories:
                continue

            estimation = gather(
                pattern.get("description", ""),
                user_id=user_id,
                group_id=group_id,
            )
            if isinstance(estimation, dict):
                confidence = float(estimation.get("confidence", 0.0))
            else:
                confidence = 0.0
            suggestion = Suggestion(
                id=str(uuid4()),
                title=pattern.get("title", ""),
                description=pattern.get("description", ""),
                confidence=confidence,
                related_entities=pattern.get("related", []),
                task_name=pattern.get("task_name"),
                context=pattern.get("context", {}),
            )
            self._suggestions[suggestion.id] = suggestion

    async def _loop(self) -> None:
        while True:
            await self.generate()
            await asyncio.sleep(self.interval)

    def start(
        self, *, user_id: str, group_id: str | None = None
    ) -> None:
        """Start the background suggestion job."""

        if self._task is None:
            self._user_id = user_id
            self._group_id = group_id
            loop = asyncio.get_running_loop()
            self._task = loop.create_task(self._loop())

    def list(self) -> List[Suggestion]:
        return [s for s in self._suggestions.values() if s.state != "dismissed"]

    def get(self, suggestion_id: str) -> Suggestion:
        return self._suggestions[suggestion_id]

    def accept(
        self,
        suggestion_id: str,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> None:
        suggestion = self.get(suggestion_id)
        suggestion.state = "accepted"
        if suggestion.task_name:
            scheduler = get_default_scheduler()
            params = inspect.signature(scheduler.run_task).parameters
            if "user_id" in params and "group_id" in params:
                if user_id is None and group_id is None:
                    scheduler.run_task(suggestion.task_name)
                elif group_id is None:
                    scheduler.run_task(suggestion.task_name, user_id=user_id)
                elif user_id is None:
                    scheduler.run_task(suggestion.task_name, group_id=group_id)
                else:
                    scheduler.run_task(
                        suggestion.task_name, user_id=user_id, group_id=group_id
                    )
            elif "user_id" in params:
                if user_id is None:
                    scheduler.run_task(suggestion.task_name)
                else:
                    scheduler.run_task(suggestion.task_name, user_id=user_id)
            elif "group_id" in params:
                if group_id is None:
                    scheduler.run_task(suggestion.task_name)
                else:
                    scheduler.run_task(suggestion.task_name, group_id=group_id)
            else:
                scheduler.run_task(suggestion.task_name)
        emit_acceptance_event(suggestion.title, user_id=user_id, group_id=group_id)
        record_suggestion_decision(
            suggestion.title, "accepted", user_id=user_id, group_id=group_id
        )

    def snooze(
        self,
        suggestion_id: str,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> None:
        suggestion = self.get(suggestion_id)
        suggestion.state = "snoozed"
        record_suggestion_decision(
            suggestion.title, "snoozed", user_id=user_id, group_id=group_id
        )

    def dismiss(
        self,
        suggestion_id: str,
        user_id: str | None = None,
        group_id: str | None = None,
    ) -> None:
        suggestion = self.get(suggestion_id)
        suggestion.state = "dismissed"
        record_suggestion_decision(
            suggestion.title, "dismissed", user_id=user_id, group_id=group_id
        )


_default_engine: SuggestionEngine | None = None


def get_default_engine() -> SuggestionEngine:
    """Return the singleton :class:`SuggestionEngine` instance."""

    global _default_engine
    if _default_engine is None:
        _default_engine = SuggestionEngine()
    return _default_engine
