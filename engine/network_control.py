"""Network fault controller for simulating real server outages.

Provides a transport-level switch that interrupts communication between the
device edge services and the remote server without affecting host networking
or local edge APIs.
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Optional

import httpx

logger = logging.getLogger("lifeline.network")


class NetworkFaultController:
    """Thread-safe controller that can drop or allow outbound server transport requests."""

    def __init__(self, offline_by_default: bool = False):
        self._offline = offline_by_default
        self._lock = threading.Lock()
        self._dropped_count = 0

    @property
    def is_offline(self) -> bool:
        with self._lock:
            return self._offline

    def set_offline(self, offline: bool) -> None:
        with self._lock:
            self._offline = offline
            logger.info("NetworkFaultController set offline=%s", offline)

    def get_dropped_count(self) -> int:
        with self._lock:
            return self._dropped_count

    def create_transport(self, base_transport: Optional[httpx.BaseTransport] = None) -> httpx.BaseTransport:
        """Create an httpx Transport that enforces real network disconnection when offline."""
        underlying = base_transport or httpx.HTTPTransport()
        controller = self

        class FaultInjectingTransport(httpx.BaseTransport):
            def handle_request(self, request: httpx.Request) -> httpx.Response:
                if controller.is_offline:
                    with controller._lock:
                        controller._dropped_count += 1
                    raise httpx.ConnectError(
                        f"Network unreachable: LIFELINE server link offline (controlled fault injection: {request.url})",
                        request=request,
                    )
                return underlying.handle_request(request)

            def close(self) -> None:
                underlying.close()

        return FaultInjectingTransport()
