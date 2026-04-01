"""
SSH remote execution via paramiko.

Provides a clean wrapper for running commands on cluster nodes,
transferring files, and starting background processes (used by
ToolRunner and FaultInjector).
"""

from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

logger = logging.getLogger(__name__)

try:
    import paramiko
    _PARAMIKO_AVAILABLE = True
except ImportError:
    _PARAMIKO_AVAILABLE = False
    logger.warning("paramiko not installed — SSHClient will fall back to subprocess (local only)")


@dataclass
class RemoteResult:
    """Result of a remote command execution."""
    stdout: str
    stderr: str
    exit_code: int

    @property
    def ok(self) -> bool:
        return self.exit_code == 0

    def check(self) -> "RemoteResult":
        """Raise RuntimeError if command failed."""
        if not self.ok:
            raise RuntimeError(
                f"Remote command failed (exit={self.exit_code}):\n"
                f"STDOUT: {self.stdout[:500]}\n"
                f"STDERR: {self.stderr[:500]}"
            )
        return self


class SSHClient:
    """
    Paramiko-backed SSH client for remote command execution on Cohesity nodes.

    Args:
        host: Target host IP or hostname.
        username: SSH username (default: cohesity).
        key_path: Path to private key file. If None, falls back to password auth.
        password: SSH password. Used if key_path is None.
        port: SSH port (default: 22).

    Note:
        host_key_policy=AutoAddPolicy for lab environments.
        Production should use RejectPolicy — document this explicitly.
    """

    def __init__(
        self,
        host: str,
        username: str = "cohesity",
        key_path: str | Path | None = None,
        password: str | None = None,
        port: int = 22,
    ) -> None:
        self._host = host
        self._username = username
        self._key_path = Path(key_path) if key_path else None
        self._password = password
        self._port = port
        self._client: "paramiko.SSHClient | None" = None

    def connect(self) -> None:
        if not _PARAMIKO_AVAILABLE:
            return
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # lab env only
        connect_kwargs: dict = {
            "hostname": self._host,
            "username": self._username,
            "port": self._port,
            "timeout": 30,
            "banner_timeout": 30,
        }
        if self._key_path and self._key_path.exists():
            connect_kwargs["key_filename"] = str(self._key_path)
        elif self._password:
            connect_kwargs["password"] = self._password
        client.connect(**connect_kwargs)
        # Enable keepalive to prevent connection drops during long tests
        transport = client.get_transport()
        if transport:
            transport.set_keepalive(60)
        self._client = client
        logger.debug("SSH connected to %s@%s:%d", self._username, self._host, self._port)

    def run(
        self,
        cmd: str | Sequence[str],
        timeout: int = 60,
        sudo: bool = False,
        check: bool = False,
    ) -> RemoteResult:
        """Execute a command and return stdout/stderr/exit_code."""
        if isinstance(cmd, (list, tuple)):
            cmd = " ".join(str(c) for c in cmd)
        if sudo and not cmd.startswith("sudo "):
            cmd = f"sudo {cmd}"

        if self._client is None:
            self.connect()

        if not _PARAMIKO_AVAILABLE or self._client is None:
            # Fallback: run locally (useful in unit tests / CI without real cluster)
            proc = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
            result = RemoteResult(stdout=proc.stdout, stderr=proc.stderr, exit_code=proc.returncode)
        else:
            stdin_, stdout_, stderr_ = self._client.exec_command(cmd, timeout=timeout)
            exit_code = stdout_.channel.recv_exit_status()
            result = RemoteResult(
                stdout=stdout_.read().decode("utf-8", errors="replace"),
                stderr=stderr_.read().decode("utf-8", errors="replace"),
                exit_code=exit_code,
            )

        logger.debug("SSH[%s] cmd=%r exit=%d", self._host, cmd[:100], result.exit_code)
        if check:
            result.check()
        return result

    def read_file(self, remote_path: str) -> str:
        """Read a text file from the remote host."""
        result = self.run(f"cat {remote_path}", check=True)
        return result.stdout

    def put_file(self, local_path: Path, remote_path: str) -> None:
        """Upload a local file to the remote host via SFTP."""
        if self._client is None:
            self.connect()
        if _PARAMIKO_AVAILABLE and self._client:
            sftp = self._client.open_sftp()
            sftp.put(str(local_path), remote_path)
            sftp.close()

    def get_file(self, remote_path: str, local_path: Path) -> None:
        """Download a remote file to local path via SFTP."""
        if self._client is None:
            self.connect()
        if _PARAMIKO_AVAILABLE and self._client:
            sftp = self._client.open_sftp()
            sftp.get(remote_path, str(local_path))
            sftp.close()

    def close(self) -> None:
        if self._client:
            self._client.close()
            self._client = None

    def __enter__(self) -> "SSHClient":
        self.connect()
        return self

    def __exit__(self, *_) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"SSHClient({self._username}@{self._host}:{self._port})"
