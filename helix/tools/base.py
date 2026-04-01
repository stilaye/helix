"""
ToolRunner Abstract Base Class.

Key design: ssh_client=None means run locally; pass an SSHClient to run on a remote node.
The same FioRunner works in CI (against locally mounted NFS) or on-node (via SSH) — zero code change.
"""

from __future__ import annotations

import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from helix.ssh.remote import SSHClient, RemoteResult


@dataclass
class RunResult:
    """Raw result of a tool execution."""
    stdout: str
    stderr: str
    exit_code: int
    command: str

    @property
    def ok(self) -> bool:
        return self.exit_code == 0


class ToolRunner(ABC):
    """
    Abstract base class for all storage tool runners.

    Args:
        ssh_client: If None, runs the tool locally via subprocess.
                    If provided, runs via SSH on the remote node.
                    This allows the same runner to work in CI or on-cluster.
    """

    def __init__(self, ssh_client: "SSHClient | None" = None) -> None:
        self._ssh = ssh_client

    @abstractmethod
    def build_command(self, **kwargs: Any) -> list[str]:
        """Build the CLI command from keyword arguments. Returns a list of strings."""

    @abstractmethod
    def parse_output(self, stdout: str, stderr: str, exit_code: int) -> Any:
        """Parse raw tool output into a typed result object."""

    def run(self, timeout: int = 600, **kwargs: Any) -> Any:
        """
        Build command, execute (locally or via SSH), parse output.

        Returns:
            Parsed result object (type depends on subclass).
        """
        cmd = self.build_command(**kwargs)
        raw = self._execute(cmd, timeout=timeout)
        return self.parse_output(raw.stdout, raw.stderr, raw.exit_code)

    def _execute(self, cmd: Sequence[str], timeout: int = 600) -> RunResult:
        """Execute command locally or via SSH."""
        cmd_str = " ".join(str(c) for c in cmd)
        if self._ssh is not None:
            result = self._ssh.run(cmd_str, timeout=timeout)
            return RunResult(
                stdout=result.stdout,
                stderr=result.stderr,
                exit_code=result.exit_code,
                command=cmd_str,
            )
        else:
            proc = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout
            )
            return RunResult(
                stdout=proc.stdout,
                stderr=proc.stderr,
                exit_code=proc.returncode,
                command=cmd_str,
            )
