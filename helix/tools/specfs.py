"""
SpecFS (SPEC SFS2014) benchmark runner.

Industry-standard filesystem benchmark with three workload profiles:
  SFS-SOW: Server Operations Workload
  SFS-OW:  Office Operations Workload
  SFS-EW:  Engineering Operations Workload

Used to validate Cohesity's filesystem performance against industry standards
and detect regressions across releases.
"""

from __future__ import annotations

import logging
import re
from enum import Enum
from typing import Any, TYPE_CHECKING

from pydantic import BaseModel

from helix.tools.base import ToolRunner

if TYPE_CHECKING:
    from helix.ssh.remote import SSHClient

logger = logging.getLogger(__name__)


class SpecFSWorkload(str, Enum):
    SOW = "sow"   # Server Operations Workload — general file server
    OW = "ow"     # Office Operations Workload — mixed office patterns
    EW = "ew"     # Engineering Operations Workload — large file workload


class SpecFSResult(BaseModel):
    """Parsed SpecFS benchmark results."""
    workload: str = ""
    throughput_ops_sec: float = 0.0
    response_time_ms: float = 0.0
    concurrent_users: int = 0
    score: float = 0.0  # SPEC score (composite)
    raw_output: str = ""


class SpecFSRunner(ToolRunner):
    """
    Run SPEC SFS2014 benchmark and parse results.

    spec_sfs2014 binary must be installed on test host or accessible via SSH.
    """

    def __init__(
        self,
        spec_path: str = "/opt/spec/bin/spec_sfs2014",
        config_dir: str = "/opt/spec/config",
        ssh_client: "SSHClient | None" = None,
    ) -> None:
        super().__init__(ssh_client)
        self._spec_path = spec_path
        self._config_dir = config_dir

    def build_command(self, **kwargs: Any) -> list[str]:
        workload = kwargs.get("workload", SpecFSWorkload.SOW)
        if isinstance(workload, SpecFSWorkload):
            workload = workload.value
        num_clients = kwargs.get("num_clients", 1)
        load_points = kwargs.get("load_points", 5)

        return [
            self._spec_path,
            "--workload", workload.upper(),
            "--clients", str(num_clients),
            "--load_points", str(load_points),
            "--output_dir", "/tmp/specfs-results",
        ]

    def parse_output(self, stdout: str, stderr: str, exit_code: int) -> SpecFSResult:
        if exit_code != 0:
            raise RuntimeError(f"spec_sfs2014 failed (exit={exit_code}): {stderr[:500]}")

        result = SpecFSResult(raw_output=stdout)

        # Parse throughput: "Throughput: 12345.67 Ops/Sec"
        m = re.search(r"Throughput:\s+([\d.]+)\s+Ops/Sec", stdout, re.IGNORECASE)
        if m:
            result.throughput_ops_sec = float(m.group(1))

        # Parse response time: "Response Time: 1.23 ms"
        m = re.search(r"Response Time:\s+([\d.]+)\s+ms", stdout, re.IGNORECASE)
        if m:
            result.response_time_ms = float(m.group(1))

        # Parse SPEC score: "SPEC SFS2014_SOW Score = 98765"
        m = re.search(r"Score\s*=\s*([\d.]+)", stdout, re.IGNORECASE)
        if m:
            result.score = float(m.group(1))

        return result
