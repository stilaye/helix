"""
FSCT (Filesystem Consistency Tool) / fsck runner.

Validates filesystem integrity post-chaos and after node failures.
dry_run() uses fsck -n (no changes), repair() uses fsck -y (auto-fix).
repair() requires the target to be unmounted first.
"""

from __future__ import annotations

import logging
import re
from typing import Any, TYPE_CHECKING

from pydantic import BaseModel

from helix.tools.base import ToolRunner

if TYPE_CHECKING:
    from helix.ssh.remote import SSHClient

logger = logging.getLogger(__name__)


class FsckResult(BaseModel):
    """Result of a filesystem check."""
    device: str = ""
    clean: bool = True
    error_count: int = 0
    warnings: list[str] = []
    repaired: bool = False
    output: str = ""


class FsctRunner(ToolRunner):
    """
    fsck wrapper for filesystem integrity validation.

    Always run via SSH on the cluster node that owns the device.
    Never run locally unless testing against a local block device.

    dry_run(): fsck -n — reports errors without fixing (safe on mounted FS)
    repair():  fsck -y — auto-fix (REQUIRES unmounting first!)
    """

    def build_command(self, **kwargs: Any) -> list[str]:
        device = kwargs.get("device", "")
        mode = kwargs.get("mode", "dry_run")
        flag = "-n" if mode == "dry_run" else "-y"
        return ["fsck", flag, device]

    def parse_output(self, stdout: str, stderr: str, exit_code: int) -> FsckResult:
        output = stdout + stderr
        device = ""

        # Exit codes: 0=clean, 1=errors-corrected, 2=errors-not-corrected, 4=operational-error
        clean = exit_code == 0
        repaired = exit_code == 1

        # Count error lines
        error_patterns = ["ERROR", "bad block", "orphaned inode", "unattached inode"]
        errors = [line for line in output.splitlines()
                  if any(p.lower() in line.lower() for p in error_patterns)]

        warnings = [line for line in output.splitlines()
                    if "WARNING" in line.upper() or "warning" in line.lower()]

        return FsckResult(
            device=device,
            clean=clean,
            error_count=len(errors),
            warnings=warnings,
            repaired=repaired,
            output=output[:2000],
        )

    def dry_run(self, device: str) -> FsckResult:
        """Run fsck -n (read-only check, safe on mounted filesystem)."""
        return self.run(device=device, mode="dry_run")

    def repair(self, device: str) -> FsckResult:
        """
        Run fsck -y (auto-repair). REQUIRES device to be unmounted first.
        Coordinate with NFSClient/iSCSIClient to unmount before calling.
        """
        logger.warning("Running fsck -y repair on %s — ensure device is unmounted", device)
        return self.run(device=device, mode="repair")
