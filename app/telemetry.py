"""Detector telemetry ingestion and deterministic contention stub for Person C boundary.

Visibly marks all synthetic measurements to ensure benchmarks are not presented as real hardware evidence.
"""

from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
import logging
import threading
import time
from typing import Any, Dict, List, Optional

import numpy as np

from shared.events import EventBus

logger = logging.getLogger("lifeline.telemetry")


class DetectorTelemetryHub:
    """Thread-safe telemetry aggregator for Person C's live detector inferences."""

    def __init__(self, history_size: int = 1000):
        self._history_size = history_size
        self._frame_times: deque[float] = deque(maxlen=history_size)
        self._latencies: deque[float] = deque(maxlen=history_size)
        self._dropped_frames_total = 0
        self._current_queue_depth = 0
        self._model_name = "yolo-v8s-inspections"
        self._last_fps = 0.0
        self._is_synthetic = False
        self._lock = threading.Lock()

    def record_frame(
        self,
        frame_latency_ms: float,
        dropped_frames: int = 0,
        queue_depth: int = 0,
        model_name: Optional[str] = None,
        is_synthetic: bool = False,
    ) -> None:
        """Record telemetry from one processed frame."""
        now = time.monotonic()
        with self._lock:
            self._frame_times.append(now)
            self._latencies.append(frame_latency_ms)
            self._dropped_frames_total += dropped_frames
            self._current_queue_depth = queue_depth
            self._is_synthetic = is_synthetic
            if model_name:
                self._model_name = model_name

            # Calculate instant FPS from recent timestamps
            if len(self._frame_times) >= 2:
                time_span = self._frame_times[-1] - self._frame_times[0]
                if time_span > 0:
                    self._last_fps = (len(self._frame_times) - 1) / time_span

    def get_metrics(self) -> Dict[str, Any]:
        """Compute summary statistics for detector health and contention."""
        with self._lock:
            latencies = list(self._latencies)
            fps = self._last_fps
            dropped = self._dropped_frames_total
            queue = self._current_queue_depth
            model = self._model_name
            is_synth = self._is_synthetic

        if latencies:
            arr = np.array(latencies)
            p50 = float(np.percentile(arr, 50))
            p95 = float(np.percentile(arr, 95))
            p99 = float(np.percentile(arr, 99))
            mean_latency = float(np.mean(arr))
        else:
            p50, p95, p99, mean_latency = 0.0, 0.0, 0.0, 0.0

        return {
            "fps": round(fps, 2),
            "mean_latency_ms": round(mean_latency, 2),
            "p50_latency_ms": round(p50, 2),
            "p95_latency_ms": round(p95, 2),
            "p99_latency_ms": round(p99, 2),
            "dropped_frames": dropped,
            "queue_depth": queue,
            "model_name": model,
            "is_synthetic": is_synth,
            "provenance": "SYNTHETIC_STUB" if is_synth else "REAL_HARDWARE_DETECTOR",
        }


class DeterministicDetectorStub:
    """Simulates real detector contention under fixed CPU/thread settings.

    VISIBLY LABELS all frames as synthetic to prevent confusion with real detector evidence.
    """

    def __init__(
        self,
        hub: DetectorTelemetryHub,
        events: Optional[EventBus] = None,
        target_fps: float = 30.0,
        workload_duration_ms: float = 15.0,
    ):
        self.hub = hub
        self.events = events
        self.target_fps = target_fps
        self.workload_duration_ms = workload_duration_ms
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True, name="DetectorStub")
        self._thread.start()

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=2.0)

    def _run_loop(self) -> None:
        frame_interval = 1.0 / self.target_fps
        next_frame_time = time.monotonic()

        while not self._stop_event.is_set():
            t0 = time.monotonic()
            # Deterministic synthetic workload: perform matrix multiplication to emulate neural net inference
            mat = np.random.randn(64, 64).astype(np.float32)
            for _ in range(5):
                mat = np.dot(mat, mat)
            # Sleep remainder of simulated inference latency
            work_elapsed = (time.monotonic() - t0) * 1000.0
            sleep_needed = max(0.0, (self.workload_duration_ms - work_elapsed) / 1000.0)
            if sleep_needed > 0:
                time.sleep(sleep_needed)

            total_latency = (time.monotonic() - t0) * 1000.0

            # Record telemetry with explicit is_synthetic=True flag
            self.hub.record_frame(
                frame_latency_ms=total_latency,
                dropped_frames=0,
                queue_depth=1,
                model_name="synthetic-yolo-v8-stub",
                is_synthetic=True,
            )

            if self.events and np.random.rand() < 0.1:  # Emit periodically
                self.events.emit(
                    "fps_update",
                    component="detector",
                    payload=self.hub.get_metrics(),
                )

            next_frame_time += frame_interval
            sleep_to_next = next_frame_time - time.monotonic()
            if sleep_to_next > 0:
                time.sleep(sleep_to_next)
            else:
                next_frame_time = time.monotonic()
