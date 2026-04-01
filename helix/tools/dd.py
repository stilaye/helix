"""
DD (Disk Dump) runner for simple throughput measurement.

Used to measure raw backup ingestion speed (GB/min) and
validate data write paths independent of higher-level protocols.
"""

from __future__ import annotations

import re
import logging
from typing import Any, TYPE_CHECKING

from pydantic import BaseModel

from helix.tools.base import ToolRunner

if TYPE_CHECKING:
    from helix.ssh.remote import SSHClient

logger = logging.getLogger(__name__)


class DDResult(BaseModel):
    """Parsed dd throughput result."""
    bytes_written: int = 0
    elapsed_secs: float = 0.0
    throughput_mbs: float = 0.0

    @property
    def throughput_gbmin(self) -> float:
        """Convert MB/s to GB/min for backup ingestion reporting."""
        return self.throughput_mbs * 60 / 1024.0


class DDRunner(ToolRunner):
    """
    Run dd and parse throughput from its stderr output.

    dd reports throughput as: "X bytes (Y GB) copied, Z s, N MB/s"
    """

    def build_command(self, **kwargs: Any) -> list[str]:
        input_file = kwargs.get("input_file", "/dev/urandom")
        output_file = kwargs.get("output_file", "/tmp/dd_test")
        block_size = kwargs.get("block_size", "1M")
        count = kwargs.get("count", 1024)          # 1 GB default
        use_direct = kwargs.get("use_direct", True)

        cmd = [
            "dd",
            f"if={input_file}",
            f"of={output_file}",
            f"bs={block_size}",
            f"count={count}",
            "conv=fsync",
        ]
        if use_direct:
            cmd.append("oflag=direct")
        return cmd

    def parse_output(self, stdout: str, stderr: str, exit_code: int) -> DDResult:
        if exit_code != 0:
            raise RuntimeError(f"dd failed (exit={exit_code}): {stderr[:500]}")

        # dd reports on stderr: "1073741824 bytes (1.1 GB) copied, 2.345 s, 458 MB/s"
        combined = stderr + stdout
        # Match: N bytes ... copied, T s, R MB/s (or GB/s)
        match = re.search(
            r"(\d+)\s+bytes.*?copied,\s+([\d.]+)\s+s,\s+([\d.]+)\s+(MB|GB|KB)/s",
            combined,
        )
        if match:
            bytes_written = int(match.group(1))
            elapsed = float(match.group(2))
            rate = float(match.group(3))
            unit = match.group(4)
            if unit == "GB":
                rate *= 1024.0
            elif unit == "KB":
                rate /= 1024.0
            return DDResult(bytes_written=bytes_written, elapsed_secs=elapsed, throughput_mbs=rate)

        # Fallback: compute from bytes and time if pattern didn't match
        bytes_match = re.search(r"(\d+)\s+bytes", combined)
        time_match = re.search(r"([\d.]+)\s+s,", combined)
        if bytes_match and time_match:
            b = int(bytes_match.group(1))
            t = float(time_match.group(1))
            mbs = (b / (1024 * 1024)) / t if t > 0 else 0.0
            return DDResult(bytes_written=b, elapsed_secs=t, throughput_mbs=mbs)

        logger.warning("Could not parse dd output: %s", combined[:300])
        return DDResult()
