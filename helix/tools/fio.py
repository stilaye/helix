"""
FIO (Flexible I/O Tester) runner.

Uses --output-format=json for machine-parseable output.
FioWorkload enum provides named profiles (no raw CLI flags in tests).
FioResult is a typed Pydantic model with all key metrics.
"""

from __future__ import annotations

import json
import logging
from enum import Enum
from typing import Any, TYPE_CHECKING

from pydantic import BaseModel

from helix.tools.base import ToolRunner

if TYPE_CHECKING:
    from helix.ssh.remote import SSHClient

logger = logging.getLogger(__name__)


class FioWorkload(str, Enum):
    """Named workload profiles. Tests reference these instead of raw CLI flags."""
    SEQUENTIAL_WRITE = "seqwrite"
    SEQUENTIAL_READ = "seqread"
    RANDOM_READ = "randread"
    RANDOM_WRITE = "randwrite"
    MIXED_OLTP = "randrw"       # 70% read / 30% write, 4KB, depth=32
    MIXED_BACKUP = "seqwrite"   # Sequential, large block, high depth


# Profile → fio parameters mapping
_WORKLOAD_PARAMS: dict[FioWorkload, dict[str, Any]] = {
    FioWorkload.SEQUENTIAL_WRITE: {
        "rw": "write", "bs": "1m", "iodepth": 16, "ioengine": "libaio",
    },
    FioWorkload.SEQUENTIAL_READ: {
        "rw": "read", "bs": "1m", "iodepth": 16, "ioengine": "libaio",
    },
    FioWorkload.RANDOM_READ: {
        "rw": "randread", "bs": "4k", "iodepth": 32, "ioengine": "libaio",
    },
    FioWorkload.RANDOM_WRITE: {
        "rw": "randwrite", "bs": "4k", "iodepth": 32, "ioengine": "libaio",
    },
    FioWorkload.MIXED_OLTP: {
        "rw": "randrw", "rwmixread": "70", "bs": "4k", "iodepth": 32, "ioengine": "libaio",
    },
    FioWorkload.MIXED_BACKUP: {
        "rw": "write", "bs": "512k", "iodepth": 64, "ioengine": "libaio",
    },
}


class FioJobSpec(BaseModel):
    """Parameters for a fio job. Tests set workload_profile; everything else has sensible defaults."""

    workload_profile: FioWorkload = FioWorkload.RANDOM_READ
    filename: str = "/tmp/fio_test_file"
    size: str = "1g"
    runtime: int = 60           # seconds
    numjobs: int = 1
    name: str = "helix-fio"
    direct: int = 1             # bypass page cache (O_DIRECT)
    group_reporting: bool = True
    # Override any profile param
    rw: str | None = None
    bs: str | None = None
    iodepth: int | None = None


class FioResult(BaseModel):
    """Typed fio output with the metrics tests actually assert against."""

    job_name: str = ""
    workload: str = ""

    # Read metrics
    read_iops: float = 0.0
    read_bw_mbs: float = 0.0
    read_lat_p50_us: float = 0.0
    read_lat_p95_us: float = 0.0
    read_lat_p99_us: float = 0.0
    read_lat_mean_us: float = 0.0

    # Write metrics
    write_iops: float = 0.0
    write_bw_mbs: float = 0.0
    write_lat_p99_us: float = 0.0
    write_lat_mean_us: float = 0.0

    # Raw JSON for Allure attachment
    raw_json: str = ""


class FioRunner(ToolRunner):
    """
    Run fio and parse its JSON output into a FioResult.

    Usage:
        runner = FioRunner(ssh_client=ssh_nodes["node-1"])  # or None for local
        result = runner.run(
            workload_profile=FioWorkload.RANDOM_READ,
            filename="/mnt/nfs-test/fio.dat",
            size="10g",
            runtime=120,
        )
        assert result.read_iops > PerformanceSLA.FIO_RAND_READ_MIN_IOPS
    """

    def __init__(self, ssh_client: "SSHClient | None" = None) -> None:
        super().__init__(ssh_client)

    def build_command(self, **kwargs: Any) -> list[str]:
        spec = FioJobSpec(**{k: v for k, v in kwargs.items() if hasattr(FioJobSpec.model_fields, k) or k in FioJobSpec.model_fields})
        if kwargs:
            spec = FioJobSpec.model_validate(kwargs)
        profile_params = _WORKLOAD_PARAMS.get(spec.workload_profile, {})

        cmd = [
            "fio",
            f"--name={spec.name}",
            f"--filename={spec.filename}",
            f"--size={spec.size}",
            f"--runtime={spec.runtime}",
            f"--numjobs={spec.numjobs}",
            f"--direct={spec.direct}",
            "--output-format=json",
        ]
        # Apply profile params (can be overridden by explicit spec fields)
        for key, val in profile_params.items():
            cmd.append(f"--{key}={val}")

        # Explicit overrides
        if spec.rw:
            cmd.append(f"--rw={spec.rw}")
        if spec.bs:
            cmd.append(f"--bs={spec.bs}")
        if spec.iodepth:
            cmd.append(f"--iodepth={spec.iodepth}")
        if spec.group_reporting:
            cmd.append("--group_reporting")

        return cmd

    def parse_output(self, stdout: str, stderr: str, exit_code: int) -> FioResult:
        if exit_code != 0:
            raise RuntimeError(f"fio failed (exit={exit_code}): {stderr[:500]}")

        try:
            data = json.loads(stdout)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"fio output is not valid JSON: {e}\nOutput: {stdout[:500]}") from e

        jobs = data.get("jobs", [])
        if not jobs:
            raise RuntimeError(f"fio produced no jobs in output. stderr: {stderr[:200]}")

        job = jobs[0]
        read = job.get("read", {})
        write = job.get("write", {})
        read_pct = read.get("clat_ns", {}).get("percentile", {})
        write_pct = write.get("clat_ns", {}).get("percentile", {})

        def ns_to_us(ns: float) -> float:
            return ns / 1000.0

        return FioResult(
            job_name=job.get("jobname", ""),
            workload=job.get("rwmixread", ""),
            read_iops=float(read.get("iops", 0)),
            read_bw_mbs=float(read.get("bw", 0)) / 1024.0,  # KB/s → MB/s
            read_lat_p50_us=ns_to_us(float(read_pct.get("50.000000", 0))),
            read_lat_p95_us=ns_to_us(float(read_pct.get("95.000000", 0))),
            read_lat_p99_us=ns_to_us(float(read_pct.get("99.000000", 0))),
            read_lat_mean_us=ns_to_us(float(read.get("clat_ns", {}).get("mean", 0))),
            write_iops=float(write.get("iops", 0)),
            write_bw_mbs=float(write.get("bw", 0)) / 1024.0,
            write_lat_p99_us=ns_to_us(float(write_pct.get("99.000000", 0))),
            write_lat_mean_us=ns_to_us(float(write.get("clat_ns", {}).get("mean", 0))),
            raw_json=stdout,
        )
