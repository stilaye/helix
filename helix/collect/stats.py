"""
StatsCollector — continuous background sampling of cluster node performance metrics.

Runs throughout the test session (not just on failure), collecting:
  - Disk I/O: %util, r/s, w/s, rkB/s, wkB/s, await per device (iostat -x)
  - CPU/memory/swap: us, sy, id, wa, run queue, block queue (vmstat)
  - Memory: used, free, buff/cache (free -m)
  - Network: rx_bytes, tx_bytes per interface (/proc/net/dev)

All samples time-stamped. Written to allure-results/host_stats.csv at end of session
for correlation with test timeline.

Usage (in root conftest.py):
    @pytest.fixture(scope="session", autouse=True)
    def stats_collector(ssh_nodes):
        collector = StatsCollector(ssh_nodes, interval_secs=5)
        collector.start()
        yield collector
        collector.stop()
        collector.write_csv("allure-results/host_stats.csv")
        collector.attach_to_allure("allure-results/host_stats.csv")
"""

from __future__ import annotations

import csv
import io
import logging
import re
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from helix.ssh.remote import SSHClient

logger = logging.getLogger(__name__)


@dataclass
class StatSample:
    timestamp: float
    node_id: str
    cpu_usr: float = 0.0
    cpu_sys: float = 0.0
    cpu_iowait: float = 0.0
    cpu_idle: float = 0.0
    mem_used_mb: float = 0.0
    mem_free_mb: float = 0.0
    mem_cached_mb: float = 0.0
    disk_read_kbs: float = 0.0
    disk_write_kbs: float = 0.0
    disk_util_pct: float = 0.0
    net_rx_bytes: float = 0.0
    net_tx_bytes: float = 0.0
    run_queue: int = 0

    @classmethod
    def empty(cls, node_id: str) -> "StatSample":
        return cls(timestamp=time.time(), node_id=node_id)


class StatsCollector:
    """
    Session-scoped background stats collector.

    Spawns one daemon thread per cluster node. Each thread SSHs into the node
    and runs iostat/vmstat/free on an interval, parsing stdout into StatSample objects.
    """

    # Combined command that runs all stats in one SSH call
    STATS_CMD = (
        "iostat -x 1 1 -o JSON 2>/dev/null; "
        "echo '---VMSTAT---'; vmstat 1 2 2>/dev/null | tail -1; "
        "echo '---MEMORY---'; free -m 2>/dev/null; "
        "echo '---NETDEV---'; cat /proc/net/dev 2>/dev/null"
    )

    def __init__(
        self,
        ssh_nodes: dict[str, "SSHClient"],
        interval_secs: int = 5,
    ) -> None:
        self._nodes = ssh_nodes
        self._interval = interval_secs
        self._samples: dict[str, list[StatSample]] = {nid: [] for nid in ssh_nodes}
        self._threads: list[threading.Thread] = []
        self._running = False
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start background sampling threads for all nodes."""
        self._running = True
        for node_id, ssh in self._nodes.items():
            t = threading.Thread(
                target=self._collect_loop,
                args=(node_id, ssh),
                daemon=True,
                name=f"stats-{node_id}",
            )
            t.start()
            self._threads.append(t)
        logger.info("StatsCollector: started sampling %d nodes every %ds", len(self._nodes), self._interval)

    def stop(self) -> None:
        """Stop all sampling threads."""
        self._running = False
        for t in self._threads:
            t.join(timeout=self._interval + 5)
        logger.info("StatsCollector: stopped. Total samples: %d", sum(len(s) for s in self._samples.values()))

    def get_samples(self, node_id: str) -> list[StatSample]:
        """Return all samples for a node."""
        return list(self._samples.get(node_id, []))

    def get_peak_disk_util(self, node_id: str) -> float:
        """Return peak disk utilization for a node (0.0 – 100.0)."""
        samples = self._samples.get(node_id, [])
        if not samples:
            return 0.0
        return max(s.disk_util_pct for s in samples)

    def get_avg_cpu_iowait(self, node_id: str) -> float:
        """Return average CPU I/O wait % — high value indicates storage bottleneck."""
        samples = self._samples.get(node_id, [])
        if not samples:
            return 0.0
        return sum(s.cpu_iowait for s in samples) / len(samples)

    def write_csv(self, path: str | Path) -> None:
        """Write all samples to a CSV file for Allure attachment."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        fieldnames = [
            "timestamp", "node_id", "cpu_usr", "cpu_sys", "cpu_iowait", "cpu_idle",
            "mem_used_mb", "mem_free_mb", "mem_cached_mb",
            "disk_read_kbs", "disk_write_kbs", "disk_util_pct",
            "net_rx_bytes", "net_tx_bytes", "run_queue",
        ]

        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            all_samples = []
            for node_samples in self._samples.values():
                all_samples.extend(node_samples)
            all_samples.sort(key=lambda s: s.timestamp)
            for sample in all_samples:
                writer.writerow({
                    "timestamp": f"{sample.timestamp:.3f}",
                    "node_id": sample.node_id,
                    "cpu_usr": f"{sample.cpu_usr:.1f}",
                    "cpu_sys": f"{sample.cpu_sys:.1f}",
                    "cpu_iowait": f"{sample.cpu_iowait:.1f}",
                    "cpu_idle": f"{sample.cpu_idle:.1f}",
                    "mem_used_mb": f"{sample.mem_used_mb:.0f}",
                    "mem_free_mb": f"{sample.mem_free_mb:.0f}",
                    "mem_cached_mb": f"{sample.mem_cached_mb:.0f}",
                    "disk_read_kbs": f"{sample.disk_read_kbs:.1f}",
                    "disk_write_kbs": f"{sample.disk_write_kbs:.1f}",
                    "disk_util_pct": f"{sample.disk_util_pct:.1f}",
                    "net_rx_bytes": f"{sample.net_rx_bytes:.0f}",
                    "net_tx_bytes": f"{sample.net_tx_bytes:.0f}",
                    "run_queue": sample.run_queue,
                })

        logger.info("StatsCollector: wrote %d samples to %s", sum(len(s) for s in self._samples.values()), path)

    def attach_to_allure(self, csv_path: str | Path) -> None:
        """Attach CSV to current Allure session."""
        try:
            import allure
            allure.attach.file(
                str(csv_path),
                name="host_stats.csv",
                attachment_type=allure.attachment_type.CSV,
            )
        except (ImportError, Exception) as e:
            logger.debug("StatsCollector: allure attach failed: %s", e)

    # ─── Sampling loop ────────────────────────────────────────────────────────

    def _collect_loop(self, node_id: str, ssh: "SSHClient") -> None:
        """Main loop: collect stats every interval_secs."""
        while self._running:
            try:
                result = ssh.run(self.STATS_CMD, check=False)
                sample = self._parse_output(node_id, result.stdout)
                with self._lock:
                    self._samples[node_id].append(sample)
            except Exception as e:
                logger.debug("StatsCollector: error sampling %s: %s", node_id, e)

            time.sleep(self._interval)

    def _parse_output(self, node_id: str, output: str) -> StatSample:
        """Parse combined command output into a StatSample."""
        sample = StatSample.empty(node_id)
        sections = output.split("---")

        for i, section in enumerate(sections):
            section = section.strip()
            if not section:
                continue

            try:
                if i == 0 and section.startswith("{"):
                    self._parse_iostat_json(section, sample)
                elif "VMSTAT" in section:
                    self._parse_vmstat(sections[i + 1] if i + 1 < len(sections) else "", sample)
                elif "MEMORY" in section:
                    self._parse_free(sections[i + 1] if i + 1 < len(sections) else "", sample)
                elif "NETDEV" in section:
                    self._parse_netdev(sections[i + 1] if i + 1 < len(sections) else "", sample)
            except Exception as e:
                logger.debug("StatsCollector: parse error in section %d for %s: %s", i, node_id, e)

        return sample

    def _parse_iostat_json(self, output: str, sample: StatSample) -> None:
        """Parse iostat -o JSON output."""
        import json
        try:
            data = json.loads(output)
            stats = data.get("sysstat", {}).get("hosts", [{}])[0].get("statistics", [{}])
            if not stats:
                return
            # CPU stats
            cpu = stats[0].get("avg-cpu", {})
            sample.cpu_usr = float(cpu.get("user", 0))
            sample.cpu_sys = float(cpu.get("system", 0))
            sample.cpu_iowait = float(cpu.get("iowait", 0))
            sample.cpu_idle = float(cpu.get("idle", 0))
            # Disk stats — aggregate across all devices
            disks = stats[0].get("disk", [])
            if disks:
                sample.disk_read_kbs = sum(float(d.get("rkB/s", 0)) for d in disks)
                sample.disk_write_kbs = sum(float(d.get("wkB/s", 0)) for d in disks)
                sample.disk_util_pct = max(float(d.get("%util", 0)) for d in disks)
        except (json.JSONDecodeError, KeyError, IndexError):
            pass

    def _parse_vmstat(self, output: str, sample: StatSample) -> None:
        """Parse vmstat 1 2 | tail -1 output."""
        for line in output.strip().splitlines():
            parts = line.split()
            if len(parts) >= 15 and parts[0].isdigit():
                try:
                    sample.run_queue = int(parts[0])
                    # parts: r b swpd free buff cache si so bi bo in cs us sy id wa
                    if len(parts) >= 16:
                        sample.cpu_usr = float(parts[12])
                        sample.cpu_sys = float(parts[13])
                        sample.cpu_idle = float(parts[14])
                        sample.cpu_iowait = float(parts[15])
                except (ValueError, IndexError):
                    pass

    def _parse_free(self, output: str, sample: StatSample) -> None:
        """Parse free -m output."""
        for line in output.strip().splitlines():
            if line.startswith("Mem:"):
                parts = line.split()
                if len(parts) >= 4:
                    sample.mem_used_mb = float(parts[2])
                    sample.mem_free_mb = float(parts[3])
                    if len(parts) >= 7:
                        sample.mem_cached_mb = float(parts[5])

    def _parse_netdev(self, output: str, sample: StatSample) -> None:
        """Parse /proc/net/dev — sum rx/tx bytes across all non-loopback interfaces."""
        rx_total = 0
        tx_total = 0
        for line in output.strip().splitlines():
            if ":" in line and "lo:" not in line:
                parts = line.split(":")[1].split()
                if len(parts) >= 9:
                    try:
                        rx_total += int(parts[0])
                        tx_total += int(parts[8])
                    except (ValueError, IndexError):
                        pass
        sample.net_rx_bytes = float(rx_total)
        sample.net_tx_bytes = float(tx_total)
