from pathlib import Path

from engine.monitor import ResourceMonitor


class Process:
    def memory_info(self):
        return type("Memory", (), {"rss": 123})()

    def cpu_percent(self, interval=None):
        return 7.5


def test_cgroup_v2_snapshot(tmp_path: Path):
    (tmp_path / "memory.current").write_text("1000")
    (tmp_path / "memory.max").write_text("2000")
    (tmp_path / "memory.stat").write_text("inactive_file 250\nanon 750\n")
    (tmp_path / "memory.events").write_text("oom 2\noom_kill 1\n")
    snap = ResourceMonitor(tmp_path, process=Process()).snapshot()
    assert snap.working_set_bytes == 750
    assert snap.memory_pressure == 0.375
    assert snap.memory_events["oom_kill"] == 1
