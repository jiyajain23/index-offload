"""Bounded video capture and red-beacon detection for supported demo scenarios (Synthetic, Live Webcam, and Remote Browser Stream)."""

from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
from queue import Empty, Full, Queue
import threading
import time
from typing import Optional, Union
import uuid

import cv2
import numpy as np

from shared.contracts import PriorityLevel, RecordEnvelope, SharingPolicy
from .embedding import MODEL_VERSION, embed

# Maximum age (seconds) of a remote-injected frame before we show "waiting" overlay
_REMOTE_FRAME_TIMEOUT = 5.0


class BeaconRunner:
    """Detect a red equipment status beacon; never infer a specific emergency."""

    def __init__(
        self,
        video_path: Union[str, Path, int],
        memory_service,
        telemetry,
        events=None,
        context_id: str = "zone_01",
        synthetic_input: bool = True,
        cooldown_seconds: float = 8.0,
    ):
        self.video_source: Union[str, int] = (
            int(video_path) if isinstance(video_path, int) or (isinstance(video_path, str) and video_path.isdigit())
            else str(video_path)
        )
        self.video_path = str(self.video_source)
        self.memory_service = memory_service
        self.telemetry = telemetry
        self.events = events
        self.context_id = context_id
        self.synthetic_input = synthetic_input
        self.cooldown_seconds = cooldown_seconds
        self.frames: Queue = Queue(maxsize=4)
        self.stop_signal = threading.Event()
        self.threads: list[threading.Thread] = []
        self._lock = threading.Lock()
        self.latest_jpeg: bytes | None = None
        self.processed_frames = 0
        self.dropped_frames = 0
        self.last_observation: dict | None = None
        self._last_event_at = 0.0
        # Remote browser stream state
        self._remote_mode: bool = self.video_source == "remote"
        self._last_remote_frame_at: float = 0.0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self):
        if self.threads:
            return
        self.stop_signal.clear()
        capture_target = self._capture_remote if self._remote_mode else self._capture
        self.threads = [
            threading.Thread(target=capture_target, daemon=True, name="BeaconCapture"),
            threading.Thread(target=self._process, daemon=True, name="BeaconDetector"),
        ]
        for thread in self.threads:
            thread.start()

    def stop(self):
        self.stop_signal.set()
        for thread in self.threads:
            thread.join(timeout=2.0)
        self.threads.clear()

    def switch_source(self, new_source: Union[str, int], synthetic: Optional[bool] = None) -> bool:
        """Switch video source between live webcam, remote browser stream, or synthetic fixture."""
        with self._lock:
            self.stop()
            while not self.frames.empty():
                try:
                    self.frames.get_nowait()
                except Empty:
                    break

            is_remote = new_source == "remote"
            is_camera = (
                not is_remote and (
                    new_source == "webcam"
                    or isinstance(new_source, int)
                    or (isinstance(new_source, str) and new_source.isdigit())
                )
            )

            if is_remote:
                self.video_source = "remote"
                self.synthetic_input = False if synthetic is None else synthetic
                self._remote_mode = True
            elif is_camera:
                dev_idx = 0 if new_source == "webcam" else int(new_source)
                self.video_source = dev_idx
                self.synthetic_input = False if synthetic is None else synthetic
                self._remote_mode = False
            else:
                self.video_source = str(new_source)
                self.synthetic_input = True if synthetic is None else synthetic
                self._remote_mode = False

            self.video_path = str(self.video_source)
            self._last_remote_frame_at = 0.0
            self.start()

            if self.events:
                self.events.emit(
                    "perception_source_switched",
                    component="detector",
                    payload={
                        "source": str(self.video_source),
                        "synthetic_input": self.synthetic_input,
                        "is_camera": is_camera or is_remote,
                        "remote_stream": is_remote,
                    },
                )
            return True

    # ------------------------------------------------------------------
    # Remote browser-stream injection
    # ------------------------------------------------------------------

    def inject_frame(self, jpeg_bytes: bytes) -> bool:
        """Accept a JPEG frame uploaded from the browser's webcam and push into the processing queue.
        
        Returns True if the frame was accepted, False if the queue is full (dropped).
        """
        arr = np.frombuffer(jpeg_bytes, dtype=np.uint8)
        frame = cv2.imdecode(arr, cv2.IMREAD_COLOR)
        if frame is None:
            return False
        with self._lock:
            self._last_remote_frame_at = time.monotonic()
        try:
            self.frames.put_nowait((frame, time.monotonic()))
            return True
        except Full:
            with self._lock:
                self.dropped_frames += 1
            return False

    # ------------------------------------------------------------------
    # Capture threads
    # ------------------------------------------------------------------

    def _open_capture(self) -> cv2.VideoCapture | None:
        try:
            if self.video_source == "webcam" or isinstance(self.video_source, int):
                idx = 0 if self.video_source == "webcam" else int(self.video_source)
                cap = cv2.VideoCapture(idx)
                if cap.isOpened():
                    cap.set(cv2.CAP_PROP_FRAME_WIDTH, 640)
                    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 360)
                return cap
            return cv2.VideoCapture(str(self.video_source))
        except Exception as e:
            if self.events:
                self.events.emit("perception_failed", component="detector",
                                 payload={"reason": f"Failed opening source {self.video_source}: {e}"})
            return None

    def _capture(self):
        """Capture thread for local file / local hardware camera."""
        capture = self._open_capture()
        if capture is None or not capture.isOpened():
            if self.events:
                self.events.emit("perception_failed", component="detector",
                                 payload={"reason": f"Video source '{self.video_source}' unavailable"})
            # Emit "no signal" frames so the UI doesn't freeze
            no_signal = _make_no_signal_frame(f"Camera '{self.video_source}' not found on this host.")
            while not self.stop_signal.is_set():
                try:
                    self.frames.put_nowait((no_signal.copy(), time.monotonic()))
                except Full:
                    pass
                self.stop_signal.wait(0.5)
            return

        raw_fps = capture.get(cv2.CAP_PROP_FPS) or 15
        fps = 15 if (raw_fps <= 0 or raw_fps > 60) else raw_fps
        interval = 1.0 / fps

        try:
            while not self.stop_signal.is_set():
                start = time.monotonic()
                ok, frame = capture.read()
                if not ok:
                    if not self.synthetic_input:
                        time.sleep(0.05)
                        continue
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

    def _capture_remote(self):
        """Capture thread for remote browser stream — frames arrive via inject_frame()."""
        waiting_frame = _make_waiting_frame()
        while not self.stop_signal.is_set():
            with self._lock:
                last = self._last_remote_frame_at
            age = time.monotonic() - last if last > 0 else float("inf")
            if age > _REMOTE_FRAME_TIMEOUT:
                # No recent browser frame — push the "waiting" placeholder
                try:
                    self.frames.put_nowait((waiting_frame.copy(), time.monotonic()))
                except Full:
                    pass
            self.stop_signal.wait(0.4)

    # ------------------------------------------------------------------
    # Detection / processing thread
    # ------------------------------------------------------------------

    def _process(self):
        consecutive = 0
        while not self.stop_signal.is_set():
            try:
                frame, captured_at = self.frames.get(timeout=0.2)
            except Empty:
                continue

            if frame.shape[0] != 360 or frame.shape[1] != 640:
                frame = cv2.resize(frame, (640, 360))

            hsv = cv2.cvtColor(frame, cv2.COLOR_BGR2HSV)
            low = cv2.inRange(hsv, (0, 100, 90), (12, 255, 255))
            high = cv2.inRange(hsv, (170, 100, 90), (179, 255, 255))
            mask = cv2.bitwise_or(low, high)
            red_pixels = int(cv2.countNonZero(mask))

            active = red_pixels > 650
            consecutive = consecutive + 1 if active else 0
            overlay = frame.copy()

            is_live = not self.synthetic_input
            if active and is_live:
                contours, _ = cv2.findContours(mask, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
                for cnt in contours:
                    if cv2.contourArea(cnt) > 200:
                        x, y, w, h = cv2.boundingRect(cnt)
                        cv2.rectangle(overlay, (x, y), (x + w, y + h), (0, 0, 255), 2)
                        cv2.putText(overlay, f"BEACON HAZARD ({red_pixels} px)", (x, max(20, y - 6)),
                                    cv2.FONT_HERSHEY_SIMPLEX, 0.45, (0, 0, 255), 1)

            cv2.putText(overlay, "RED STATUS BEACON" if active else "BEACON CLEAR", (16, 34),
                        cv2.FONT_HERSHEY_SIMPLEX, 0.65,
                        (60, 80, 255) if active else (110, 220, 130), 2)

            if self.synthetic_input:
                cv2.putText(overlay, "SYNTHETIC VIDEO - LIVE CV DETECTION", (16, 341),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.48, (250, 250, 250), 1)
            elif self._remote_mode:
                cv2.putText(overlay, f"BROWSER WEBCAM STREAM - REAL-TIME CV ({red_pixels} px)", (16, 341),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.48, (0, 255, 180), 1)
            else:
                cv2.putText(overlay, f"LIVE WEBCAM - REAL-TIME CV INSPECTION ({red_pixels} px)", (16, 341),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.48, (0, 255, 255), 1)

            ok, jpeg = cv2.imencode(".jpg", overlay, [cv2.IMWRITE_JPEG_QUALITY, 75])
            latency = (time.monotonic() - captured_at) * 1000

            with self._lock:
                if ok:
                    self.latest_jpeg = jpeg.tobytes()
                self.processed_frames += 1
                drops = self.dropped_frames
                self.dropped_frames = 0

            if self._remote_mode:
                detector_name = "Browser Webcam Stream — HSV Red-Beacon Detector"
            elif not self.synthetic_input:
                detector_name = "Live Webcam HSV Red-Beacon Detector"
            else:
                detector_name = "OpenCV HSV red-beacon detector"

            self.telemetry.record_frame(
                latency,
                dropped_frames=drops,
                queue_depth=self.frames.qsize(),
                model_name=detector_name,
                is_synthetic=self.synthetic_input,
            )

            if consecutive >= 3 and time.monotonic() - self._last_event_at >= self.cooldown_seconds:
                self._last_event_at = time.monotonic()
                self._record_event(red_pixels)

    def _record_event(self, red_pixels: int):
        if self._remote_mode:
            text = "Red equipment status beacon observed via browser webcam stream in Zone 01"
            source_ref = "browser://webcam"
            scenario = "remote browser webcam stream"
        elif not self.synthetic_input:
            text = "Red equipment status beacon observed by live camera inspection in Zone 01"
            source_ref = "webcam://0"
            scenario = "live webcam inspection"
        else:
            text = "Red equipment status beacon observed at Pump P-101 in Zone 01"
            source_ref = self.video_path
            scenario = "synthetic video fixture"

        dense, indices, values = embed(text)
        record = RecordEnvelope(
            operation_id=str(uuid.uuid4()),
            entity_id="pump_p101",
            context_id=self.context_id,
            device_id="beacon_cv_demo",
            observation={
                "claim": text,
                "red_pixel_area": red_pixels,
                "scenario": scenario,
                "requires_operator_review": True,
            },
            source_type="remote_webcam" if self._remote_mode else ("live_webcam" if not self.synthetic_input else "beacon_cv"),
            source_reference=source_ref,
            observed_at=datetime.now(timezone.utc),
            sharing=SharingPolicy.PERMITTED_SHARED,
            priority=PriorityLevel.ROUTINE,
            embedding_model_version=MODEL_VERSION,
            dense_vector=dense,
            sparse_indices=indices,
            sparse_values=values,
        )
        try:
            receipt = self.memory_service.write(record)
            with self._lock:
                self.last_observation = {
                    "record_id": record.record_id,
                    "operation_id": record.operation_id,
                    "durable": receipt.durable,
                    "projection_status": receipt.projection_status,
                    "at": record.observed_at.isoformat(),
                }
            if self.events:
                self.events.emit(
                    "observation_recorded",
                    component="detector",
                    payload={
                        "record_id": record.record_id,
                        "synthetic_input": self.synthetic_input,
                        "is_camera": not self.synthetic_input,
                        "remote_stream": self._remote_mode,
                        "projection_status": receipt.projection_status,
                    },
                )
        except Exception as exc:
            if self.events:
                self.events.emit(
                    "perception_write_failed",
                    component="detector",
                    payload={"error": str(exc)},
                )

    def status(self) -> dict:
        with self._lock:
            return {
                "running": any(t.is_alive() for t in self.threads),
                "video_source": str(self.video_source),
                "source": "remote" if self._remote_mode else ("webcam" if not self.synthetic_input else "fixture"),
                "synthetic_input": self.synthetic_input,
                "is_camera": not self.synthetic_input,
                "remote_stream": self._remote_mode,
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


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _make_no_signal_frame(reason: str = "Camera unavailable") -> np.ndarray:
    img = np.zeros((360, 640, 3), dtype=np.uint8)
    cv2.putText(img, "NO SIGNAL", (200, 155), cv2.FONT_HERSHEY_SIMPLEX, 1.6, (0, 0, 220), 3)
    cv2.putText(img, reason, (30, 210), cv2.FONT_HERSHEY_SIMPLEX, 0.55, (140, 140, 140), 1)
    cv2.putText(img, "Use 'Browser Webcam' mode to stream from your device.", (30, 250),
                cv2.FONT_HERSHEY_SIMPLEX, 0.5, (80, 180, 120), 1)
    return img


def _make_waiting_frame() -> np.ndarray:
    img = np.zeros((360, 640, 3), dtype=np.uint8)
    cv2.putText(img, "WAITING FOR BROWSER STREAM", (110, 160), cv2.FONT_HERSHEY_SIMPLEX, 0.8, (0, 200, 160), 2)
    cv2.putText(img, "Allow camera access and click 'Browser Webcam' to begin.", (60, 205),
                cv2.FONT_HERSHEY_SIMPLEX, 0.52, (150, 150, 150), 1)
    return img


