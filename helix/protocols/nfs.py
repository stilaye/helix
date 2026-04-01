"""
NFS protocol client.

Mounts NFS exports via subprocess with configurable mount options.
Key feature: stale file handle recovery — catches OSError(ESTALE),
unmounts/remounts, and retries the failed operation once.
"""

from __future__ import annotations

import errno
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from helix.protocols.base import ProtocolClient

logger = logging.getLogger(__name__)


class NFSClient(ProtocolClient):
    """
    NFS client with stale file handle recovery.

    Default mount options tuned for Cohesity NFS testing:
      - nfsvers=4 (NFSv4 — stateful, ACL support)
      - hard,intr (keep retrying on network issues, interruptible)
      - rw (read-write access)

    Stale handle recovery: If a read/write raises OSError(ESTALE),
    the client automatically unmounts, remounts, and retries once.
    Tests can also explicitly call recover_stale_handle() to test the flow.
    """

    DEFAULT_MOUNT_OPTS = "nfsvers=4,hard,intr,rw,timeo=30,retrans=3"

    def __init__(self, config: Any) -> None:
        if hasattr(config, "nfs_server"):
            self._server = config.nfs_server
            self._export = config.nfs_export
            self._mount_opts = getattr(config, "nfs_mount_opts", self.DEFAULT_MOUNT_OPTS)
        else:
            self._server = config.get("nfs_server", "")
            self._export = config.get("nfs_export", "/")
            self._mount_opts = config.get("nfs_mount_opts", self.DEFAULT_MOUNT_OPTS)

        self._mount_dir: Path | None = None
        self._temp_dir_obj = None

    @property
    def protocol_name(self) -> str:
        return "nfs"

    @property
    def mount_point(self) -> Path | None:
        return self._mount_dir

    def connect(self) -> None:
        self._temp_dir_obj = tempfile.TemporaryDirectory(prefix="helix-nfs-")
        self._mount_dir = Path(self._temp_dir_obj.name)
        nfs_path = f"{self._server}:{self._export}"
        result = subprocess.run(
            ["mount", "-t", "nfs4", "-o", self._mount_opts, nfs_path, str(self._mount_dir)],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"NFS mount failed: {result.stderr}")
        logger.info("NFS mounted %s → %s (opts=%s)", nfs_path, self._mount_dir, self._mount_opts)

    def disconnect(self) -> None:
        if self._mount_dir:
            subprocess.run(["umount", "-f", "-l", str(self._mount_dir)], capture_output=True)
        if self._temp_dir_obj:
            self._temp_dir_obj.cleanup()
        self._mount_dir = None
        self._temp_dir_obj = None

    def write_file(self, remote_path: str, data: bytes) -> None:
        try:
            path = self._resolve(remote_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        except OSError as e:
            if e.errno == errno.ESTALE:
                logger.warning("Stale NFS file handle on write — recovering")
                self.recover_stale_handle()
                self._resolve(remote_path).write_bytes(data)
            else:
                raise

    def read_file(self, remote_path: str) -> bytes:
        try:
            return self._resolve(remote_path).read_bytes()
        except OSError as e:
            if e.errno == errno.ESTALE:
                logger.warning("Stale NFS file handle on read — recovering")
                self.recover_stale_handle()
                return self._resolve(remote_path).read_bytes()
            raise

    def list_directory(self, remote_path: str) -> list[str]:
        return [p.name for p in self._resolve(remote_path).iterdir()]

    def delete_file(self, remote_path: str) -> None:
        self._resolve(remote_path).unlink(missing_ok=True)

    def mkdir(self, remote_path: str) -> None:
        self._resolve(remote_path).mkdir(parents=True, exist_ok=True)

    def exists(self, remote_path: str) -> bool:
        return self._resolve(remote_path).exists()

    def recover_stale_handle(self) -> None:
        """
        Recover from a stale NFS file handle by unmounting and remounting.
        Automatically called on OSError(ESTALE); can also be called explicitly in tests.
        """
        logger.info("Recovering stale NFS handle: unmounting and remounting")
        mount_dir = self._mount_dir
        temp_obj = self._temp_dir_obj
        if mount_dir:
            subprocess.run(["umount", "-f", "-l", str(mount_dir)], capture_output=True)
        # Reconnect
        self.connect()

    def get_export_list(self) -> list[str]:
        """Get list of NFS exports from the server via showmount."""
        result = subprocess.run(
            ["showmount", "-e", self._server],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            return []
        # Parse showmount output: "/path  host1,host2" format
        exports = []
        for line in result.stdout.splitlines():
            parts = line.split()
            if parts and parts[0].startswith("/"):
                exports.append(parts[0])
        return exports

    def _resolve(self, remote_path: str) -> Path:
        if self._mount_dir is None:
            raise RuntimeError("NFSClient not connected")
        return self._mount_dir / remote_path.lstrip("/")
