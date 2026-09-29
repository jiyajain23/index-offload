"""Shared event bus and typed event publisher.

Ensures listener failures are caught and logged without disrupting storage or engine operations.
Maintains a bounded ring buffer of recent events for reconnecting UI/dashboards.
"""

from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
import logging
import threading
from typing import Any, Callable, Dict, List, Optional
import uuid

from .contracts import LifelineEvent

logger = logging.getLogger("lifeline.events")


class EventBus:
    """Thread-safe event bus with bounded history and isolated listeners."""

    def __init__(self, max_history: int = 500):
        self._max_history = max_history
        self._history: deque[LifelineEvent] = deque(maxlen=max_history)
        self._subscribers: List[Callable[[LifelineEvent], None]] = []
        self._lock = threading.Lock()

    def subscribe(self, listener: Callable[[LifelineEvent], None]) -> Callable[[], None]:
        with self._lock:
            self._subscribers.append(listener)

        def unsubscribe() -> None:
            with self._lock:
                if listener in self._subscribers:
                    self._subscribers.remove(listener)

        return unsubscribe

    def emit(
        self,
        event_type: str,
        component: str,
        payload: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
    ) -> LifelineEvent:
        event = LifelineEvent(
            event_id=str(uuid.uuid4()),
            schema_version="v1",
            emitted_at=datetime.now(timezone.utc),
            component=component,
            correlation_id=correlation_id or str(uuid.uuid4()),
            event_type=event_type,
            payload=payload or {},
        )

        with self._lock:
            self._history.append(event)
            listeners = list(self._subscribers)

        # Notify listeners outside the lock.
        # CRITICAL: Isolate listener errors so storage / engine operations are never corrupted.
        for listener in listeners:
            try:
                listener(event)
            except Exception as e:
                logger.warning("Listener %s raised exception on event %s: %s", listener, event_type, e)

        return event

    def get_history(self, limit: Optional[int] = None) -> List[LifelineEvent]:
        with self._lock:
            events = list(self._history)
        if limit is not None:
            return events[-limit:]
        return events

    def clear(self) -> None:
        with self._lock:
            self._history.clear()
