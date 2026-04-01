"""
VDBench (Virtual Database Benchmark) runner.

Generates vdbench config files from VdbenchSpec Pydantic models.
No manual .conf files — workload config is version-controlled as Python objects.
Parses CSV output into VdbenchResult with per-interval samples.
"""

from __future__ import annotations

import csv
import io
import logging
import tempfile
from enum import Enum
from pathlib import Path
from typing import Any, TYPE_CHECKING

from pydantic import BaseModel

from helix.tools.base import ToolRunner

if TYPE_CHECKING:
    from helix.ssh.remote import SSHClient

logger = logging.getLogger(__name__)


class VdbenchWorkload(str, Enum):
    OLTP = "oltp"           # Random I/O, small block, high concurrency
    DW = "dw"               # Data warehouse: sequential, large block
    MIXED = "mixed"         # Configurable mix


class VdbenchSpec(BaseModel):
    workload: VdbenchWorkload = VdbenchWorkload.OLTP
    anchor: str = "/mnt/test"
    iorate: int = 1000          # operations/sec (0 = max)
    rdpct: int = 70             # read percentage
    xfersize: str | None = None # block size (None = use workload default)
    threads: int = 8            # concurrent threads
    elapsed: int = 60           # seconds
    warmup: int = 10            # warmup seconds (excluded from stats)
    depth: int = 3              # directory tree depth
    width: int = 4              # directories per level
    files: int = 20             # files per leaf directory
    filesize: str = "64k"       # individual file size


_WORKLOAD_DEFAULTS: dict[VdbenchWorkload, dict[str, Any]] = {
    VdbenchWorkload.OLTP: {
        "rdpct": 90, "xfersize": "8k", "threads": 16,
    },
    VdbenchWorkload.DW: {
        "rdpct": 80, "xfersize": "256k", "threads": 4,
    },
    VdbenchWorkload.MIXED: {
        "rdpct": 70, "xfersize": "64k", "threads": 8,
    },
}


class VdbenchResult(BaseModel):
    """Parsed vdbench metrics from output CSV."""
    iops: float = 0.0               # operations per second
    throughput_mbs: float = 0.0     # MB/s
    avg_response_ms: float = 0.0    # average response time in ms
    p99_response_ms: float = 0.0    # P99 response time in ms
    cpu_pct: float = 0.0
    workload: str = ""
    samples: list[dict] = []


class VdbenchRunner(ToolRunner):
    """
    Run vdbench with a generated config file and parse CSV output.

    The config file is generated from VdbenchSpec — no manual .conf editing.
    vdbench must be installed on the target host (or accessible via SSH path).
    """

    def __init__(
        self,
        vdbench_path: str = "/opt/vdbench/vdbench",
        ssh_client: "SSHClient | None" = None,
    ) -> None:
        super().__init__(ssh_client)
        self._vdbench_path = vdbench_path

    def build_command(self, **kwargs: Any) -> list[str]:
        spec = VdbenchSpec.model_validate(kwargs)
        defaults = _WORKLOAD_DEFAULTS.get(spec.workload, {})

        rdpct = kwargs.get("rdpct", defaults.get("rdpct", spec.rdpct))
        xfersize = kwargs.get("xfersize", defaults.get("xfersize", spec.xfersize or "64k"))
        threads = kwargs.get("threads", defaults.get("threads", spec.threads))

        # Write config to a temp file
        config_content = self._generate_config(spec, rdpct, xfersize, threads)
        tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".conf", prefix="helix-vdbench-", delete=False
        )
        tmp.write(config_content)
        tmp.flush()
        self._config_path = tmp.name
        tmp.close()

        return [self._vdbench_path, "-f", self._config_path, "-o", "/tmp/vdbench-output"]

    def _generate_config(
        self,
        spec: VdbenchSpec,
        rdpct: int,
        xfersize: str,
        threads: int,
    ) -> str:
        lines = [
            f"hd=default,vdbench={self._vdbench_path},user=root",
            "",
            f"fsd=fsd1,anchor={spec.anchor},depth={spec.depth},"
            f"width={spec.width},files={spec.files},size={spec.filesize}",
            "",
            f"fwd=fwd1,fsd=fsd1,operation=read,xfersize={xfersize},"
            f"fileio=random,fileselect=random,threads={threads},rdpct={rdpct}",
            "",
            f"rd=rd1,fwd=fwd*,iorate={spec.iorate},elapsed={spec.elapsed},"
            f"warmup={spec.warmup},interval=1",
        ]
        return "\n".join(lines)

    def parse_output(self, stdout: str, stderr: str, exit_code: int) -> VdbenchResult:
        if exit_code != 0:
            raise RuntimeError(f"vdbench failed (exit={exit_code}): {stderr[:500]}")

        # Parse the summary line from vdbench output
        # vdbench prints: "Interval Reqrate  Read-resp Write-resp  Read-rate Write-rate  CPU%"
        samples = []
        for line in stdout.splitlines():
            parts = line.split()
            if parts and parts[0].isdigit():
                try:
                    samples.append({
                        "interval": int(parts[0]),
                        "req_rate": float(parts[1]),
                        "read_resp_ms": float(parts[2]),
                        "write_resp_ms": float(parts[3]) if len(parts) > 3 else 0.0,
                        "cpu_pct": float(parts[-1]) if len(parts) > 5 else 0.0,
                    })
                except (ValueError, IndexError):
                    pass

        if not samples:
            return VdbenchResult(raw_output=stdout)

        # Average over all (non-warmup) samples
        ops = [s["req_rate"] for s in samples]
        resp = [s["read_resp_ms"] for s in samples]
        resp_sorted = sorted(resp)
        p99_idx = max(0, int(len(resp_sorted) * 0.99) - 1)

        return VdbenchResult(
            throughput_ops_sec=sum(ops) / len(ops),
            response_time_ms=sum(resp) / len(resp),
            response_time_ms_p99=resp_sorted[p99_idx],
            cpu_pct=sum(s["cpu_pct"] for s in samples) / len(samples),
            samples=samples,
        )
