"""Generate a fixed, clearly synthetic inspection clip with a blinking red lamp."""

from pathlib import Path
import cv2
import numpy as np


def make_fixture(path: str | Path, frames: int = 180, fps: int = 15) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    writer = cv2.VideoWriter(str(path), cv2.VideoWriter_fourcc(*"MJPG"), fps, (640, 360))
    if not writer.isOpened():
        raise RuntimeError("OpenCV could not open the MJPG video writer")
    try:
        for index in range(frames):
            image = np.full((360, 640, 3), (33, 37, 42), dtype=np.uint8)
            cv2.rectangle(image, (84, 90), (560, 285), (70, 81, 89), -1)
            cv2.rectangle(image, (110, 112), (538, 255), (45, 52, 60), -1)
            cv2.putText(image, "SYNTHETIC INSPECTION FIXTURE", (110, 136),
                        cv2.FONT_HERSHEY_SIMPLEX, .52, (235, 235, 235), 1)
            cv2.putText(image, f"Pump P-101  /  frame {index:03d}", (110, 232),
                        cv2.FONT_HERSHEY_SIMPLEX, .52, (225, 225, 225), 1)
            lit = (index % 90) >= 30 and (index % 90) < 66
            cv2.circle(image, (469, 180), 25, (0, 0, 220) if lit else (28, 28, 60), -1)
            cv2.circle(image, (469, 180), 30, (145, 145, 145), 2)
            writer.write(image)
    finally:
        writer.release()
    return path


if __name__ == "__main__":
    make_fixture("fixtures/inspection_beacon.avi")
