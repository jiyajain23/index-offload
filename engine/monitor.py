"""Process and cgroup-v2 resource measurements used by the engine."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
from typing import Mapping

import psutil


@dataclass(frozen=True)
class ResourceSnapshot:
    rss_bytes: int
    working_set_bytes: int
    memory_current_bytes: int | None
    memory_max_bytes: int | None
    memory_pressure: float | None
    memory_events: Mapping[str, int]
    cpu_percent: float


class ResourceMonitor:
    """Reads cgroup v2 when present, with a process-only fallback for local runs."""

    def __init__(self, cgroup_path: str | os.PathLike[str] = "/sys/fs/cgroup", process=None):
        self._cgroup_path = Path(cgroup_path)
        self._process = process or psutil.Process()

    def snapshot(self) -> ResourceSnapshot:
        rss = self._process.memory_info().rss
        current = self._read_int("memory.current")
        maximum = self._read_limit("memory.max")
        stat = self._read_key_values("memory.stat")
        working_set = (current - stat.get("inactive_file", 0)) if current is not None else rss
        working_set = max(0, working_set)
        pressure = (working_set / maximum) if maximum else None
        return ResourceSnapshot(
            rss_bytes=rss,
            working_set_bytes=working_set,
            memory_current_bytes=current,
            memory_max_bytes=maximum,
            memory_pressure=pressure,
            memory_events=self._read_key_values("memory.events"),
            cpu_percent=self._process.cpu_percent(interval=None),
        )

    def _read_text(self, name: str) -> str | None:
        try:
            return (self._cgroup_path / name).read_text(encoding="utf-8").strip()
        except (OSError, ValueError):
            return None

    def _read_int(self, name: str) -> int | None:
        raw = self._read_text(name)
        return int(raw) if raw and raw.isdigit() else None

    def _read_limit(self, name: str) -> int | None:
        raw = self._read_text(name)
        return int(raw) if raw and raw != "max" and raw.isdigit() else None

    def _read_key_values(self, name: str) -> dict[str, int]:
        raw = self._read_text(name)
        if not raw:
            return {}
        values = {}
        for line in raw.splitlines():
            key, value = line.split(maxsplit=1)
            values[key] = int(value)
        return values
