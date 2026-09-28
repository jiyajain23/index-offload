"""Small synchronous event interface for engine observability."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable


@dataclass(frozen=True)
class EngineEvent:
    type: str
    payload: dict[str, Any]
    occurred_at: datetime


class EventEmitter:
    def __init__(self):
        self.history: list[EngineEvent] = []
        self._listeners: list[Callable[[EngineEvent], None]] = []

    def subscribe(self, listener: Callable[[EngineEvent], None]) -> Callable[[], None]:
        self._listeners.append(listener)
        return lambda: self._listeners.remove(listener)

    def emit(self, event_type: str, **payload: Any) -> EngineEvent:
        event = EngineEvent(event_type, payload, datetime.now(timezone.utc))
        self.history.append(event)
        for listener in tuple(self._listeners):
            listener(event)
        return event
