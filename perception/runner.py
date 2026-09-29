"""Bounded video capture and red-beacon detection for one supported demo scenario."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from queue import Empty, Full, Queue
import threading
import time
import uuid

import cv2
import numpy as np

from shared.contracts import PriorityLevel, RecordEnvelope, SharingPolicy
from .embedding import MODEL_VERSION, embed


class BeaconRunner:
    """Detect a red equipment status beacon; never infer a specific emergency."""

    def __init__(self, video_path: str | Path, memory_service, telemetry,
                 events=None, context_id: str = "zone_01", synthetic_input: bool = True,
                 cooldown_seconds: float = 8.0):
        self.video_path = str(video_path)
        self.memory_service = memory_service
        self.telemetry = telemetry
        self.events = events
        self.context_id = context_id
        self.synthetic_input = synthetic_input
        self.cooldown_seconds = cooldown_seconds
        self.frames: Queue = Queue(maxsize=2)
        self.stop_signal = threading.Event()
        self.threads: list[threading.Thread] = []
        self._lock = threading.Lock()
        self.latest_jpeg: bytes | None = None
        self.processed_frames = 0
        self.dropped_frames = 0
        self.last_observation: dict | None = None
        self._last_event_at = 0.0

    def start(self):
        if self.threads:
            return
        self.stop_signal.clear()
        self.threads = [
            threading.Thread(target=self._capture, daemon=True, name="BeaconCapture"),
            threading.Thread(target=self._process, daemon=True, name="BeaconDetector"),
        ]
        for thread in self.threads:
            thread.start()

    def stop(self):
        self.stop_signal.set()
        for thread in self.threads:
            thread.join(timeout=2.0)
        self.threads.clear()

    def _capture(self):
        capture = cv2.VideoCapture(self.video_path)
        if not capture.isOpened():
            if self.events:
                self.events.emit("perception_failed", component="detector",
                                 payload={"reason": "Video source unavailable"})
            return
        fps = capture.get(cv2.CAP_PROP_FPS) or 15
        interval = 1.0 / max(1, min(fps, 60))
        try:
            while not self.stop_signal.is_set():
                start = time.monotonic()
                ok, frame = capture.read()
                if not ok:
                    if not self.synthetic_input:
                        break
                    capture.set(cv2.CAP_PROP_POS_FRAMES, 0)
                    continue
                try:
                    self.frames.put_nowait((frame, start))
                except Full:
                    with self._lock:
                        self.dropped_frames += 1
                self.stop_signal.wait(max(0, interval - (time.monotonic() - start)))
        finally:
            capture.release()

    def _process(self):
        consecutive = 0
        while not self.stop_signal.is_set():
            try:
                frame, captured_at = self.frames.get(timeout=.2)
            except Empty:
                continue
            # HSV red has two ranges around hue wraparound; a minimum area
            # rejects isolated red pixels from compression/noise.
            hsv = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)
            low = cv2.inRange(hsv, (0, 100, 90), (12, 255, 255))
            high = cv2.inRange(hsv, (170, 100, 90), (179, 255, 255))
            red_pixels = int(cv2.countNonZero(cv2.bitwise_or(low, high)))
            active = red_pixels > 650
            consecutive = consecutive + 1 if active else 0
            overlay = frame.copy()
            cv2.putText(overlay, "RED STATUS BEACON" if active else "BEACON CLEAR", (16, 34),
                        cv2.FONT_HERSHEY_SIMPLEX, .65,
                        (60, 80, 255) if active else (110, 220, 130), 2)
            if self.synthetic_input:
                cv2.putText(overlay, "SYNTHETIC VIDEO - LIVE CV DETECTION", (16, 341),
                            cv2.FONT_HERSHEY_SIMPLEX, .48, (250, 250, 250), 1)
            ok, jpeg = cv2.imencode(".jpg", overlay, [cv2.IMWRITE_JPEG_QUALITY, 75])
            latency = (time.monotonic() - captured_at) * 1000
            with self._lock:
                if ok:
                    self.latest_jpeg = jpeg.tobytes()
                self.processed_frames += 1
                drops = self.dropped_frames
                self.dropped_frames = 0
            self.telemetry.record_frame(latency, dropped_frames=drops,
                                        queue_depth=self.frames.qsize(),
                                        model_name="OpenCV HSV red-beacon detector",
                                        is_synthetic=self.synthetic_input)
            if consecutive >= 3 and time.monotonic() - self._last_event_at >= self.cooldown_seconds:
                self._last_event_at = time.monotonic()
                self._record_event(red_pixels)

    def _record_event(self, red_pixels: int):
        text = "Red equipment status beacon observed at Pump P-101 in Zone 01"
        dense, indices, values = embed(text)
        record = RecordEnvelope(
            operation_id=str(uuid.uuid4()), entity_id="pump_p101",
            context_id=self.context_id, device_id="beacon_cv_demo",
            observation={"claim": text, "red_pixel_area": red_pixels,
                         "scenario": "synthetic video fixture" if self.synthetic_input else "live video",
                         "requires_operator_review": True},
            source_type="beacon_cv", source_reference=self.video_path,
            observed_at=datetime.now(timezone.utc),
            sharing=SharingPolicy.LOCAL_ONLY, priority=PriorityLevel.ROUTINE,
            embedding_model_version=MODEL_VERSION, dense_vector=dense,
            sparse_indices=indices, sparse_values=values,
        )
        try:
            receipt = self.memory_service.write(record)
            with self._lock:
                self.last_observation = {"record_id": record.record_id,
                                         "operation_id": record.operation_id,
                                         "durable": receipt.durable,
                                         "projection_status": receipt.projection_status,
                                         "at": record.observed_at.isoformat()}
            if self.events:
                self.events.emit("observation_recorded", component="detector",
                                 payload={"record_id": record.record_id,
                                          "synthetic_input": self.synthetic_input,
                                          "projection_status": receipt.projection_status})
        except Exception as exc:
            if self.events:
                self.events.emit("perception_write_failed", component="detector",
                                 payload={"error": str(exc)})

    def status(self) -> dict:
        with self._lock:
            return {
                "running": any(t.is_alive() for t in self.threads),
                "video_source": self.video_path,
                "synthetic_input": self.synthetic_input,
                "processed_frames": self.processed_frames,
                "frames": self.processed_frames,
                "drops": 0,
                "depth": 0,
                "metrics": {
                    "frames": self.processed_frames,
                    "drops": 0,
                    "depth": 0,
                },
                "latest_observation": self.last_observation,
            }
