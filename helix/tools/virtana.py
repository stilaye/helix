"""
Virtana (Virtualization Analytics) client.

Monitors storage performance impact in virtualized environments (vSphere, Hyper-V).
Used to validate that Cohesity backup jobs don't cause unacceptable VM storage latency.
"""

from __future__ import annotations

import logging
from typing import Any

from pydantic import BaseModel
from helix.tools.base import ToolRunner

logger = logging.getLogger(__name__)


class VMStorageMetrics(BaseModel):
    """Storage metrics as observed by a VM (Virtana perspective)."""
    vm_name: str = ""
    avg_read_latency_ms: float = 0.0
    avg_write_latency_ms: float = 0.0
    peak_latency_ms: float = 0.0
    iops_read: float = 0.0
    iops_write: float = 0.0
    throughput_mbs: float = 0.0
    is_hotspot: bool = False


class VirtanaResult(BaseModel):
    """Aggregated Virtana analysis results."""
    vm_metrics: list[VMStorageMetrics] = []
    hotspots: list[str] = []
    max_latency_ms: float = 0.0
    avg_latency_ms: float = 0.0
    raw_output: str = ""


class VirtanaClient(ToolRunner):
    """
    Virtana storage analytics wrapper.

    Virtana exposes a REST API or CLI for querying VM I/O patterns.
    This runner wraps the Virtana CLI if available, or the REST API.

    Primary use case: during backup window tests, verify that production VM
    latency stays within acceptable bounds despite the backup I/O load.
    """

    def __init__(
        self,
        virtana_host: str = "",
        virtana_api_key: str = "",
        ssh_client: Any = None,
    ) -> None:
        super().__init__(ssh_client)
        self._virtana_host = virtana_host
        self._api_key = virtana_api_key

    def build_command(self, **kwargs: Any) -> list[str]:
        host = kwargs.get("host", self._virtana_host)
        duration = kwargs.get("duration_secs", 60)
        return [
            "virtana-cli", "collect",
            "--host", host,
            "--duration", str(duration),
            "--output-format", "json",
        ]

    def parse_output(self, stdout: str, stderr: str, exit_code: int) -> VirtanaResult:
        result = VirtanaResult(raw_output=stdout)
        if exit_code != 0:
            logger.warning("Virtana collection had warnings: %s", stderr[:200])

        try:
            import json
            data = json.loads(stdout)
            vms = data.get("vms", [])
            for vm in vms:
                metrics = VMStorageMetrics(
                    vm_name=vm.get("name", ""),
                    avg_read_latency_ms=float(vm.get("avgReadLatencyMs", 0)),
                    avg_write_latency_ms=float(vm.get("avgWriteLatencyMs", 0)),
                    peak_latency_ms=float(vm.get("peakLatencyMs", 0)),
                    iops_read=float(vm.get("iopsRead", 0)),
                    iops_write=float(vm.get("iopsWrite", 0)),
                    throughput_mbs=float(vm.get("throughputMBs", 0)),
                    is_hotspot=bool(vm.get("isHotspot", False)),
                )
                result.vm_metrics.append(metrics)
                if metrics.is_hotspot:
                    result.hotspots.append(metrics.vm_name)

            all_latencies = [m.peak_latency_ms for m in result.vm_metrics]
            if all_latencies:
                result.max_latency_ms = max(all_latencies)
                result.avg_latency_ms = sum(all_latencies) / len(all_latencies)
        except Exception:
            pass

        return result

    def get_vm_metrics(self, duration_secs: int = 60) -> VirtanaResult:
        """Collect VM storage metrics for the specified duration."""
        return self.run(duration_secs=duration_secs)
