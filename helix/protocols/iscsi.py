"""
iSCSI protocol client.

Uses `iscsiadm` CLI for target discovery, login, and logout.
After login, inspects /sys/block to find the newly attached block device.
Provides format_device() and mount_device() for block-level I/O tests.
"""

from __future__ import annotations

import logging
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

from helix.protocols.base import ProtocolClient

logger = logging.getLogger(__name__)


class iSCSIClient(ProtocolClient):
    """
    iSCSI initiator client for block-level storage tests.

    Wraps iscsiadm CLI (must be installed on test host).
    Workflow: discover → login → find device → format → mount → test → logout
    """

    def __init__(self, config: Any) -> None:
        if hasattr(config, "iscsi_portal"):
            self._portal = config.iscsi_portal          # "10.0.0.100:3260"
            self._target_iqn = config.iscsi_target_iqn  # "iqn.2020-01.com.cohesity:test"
        else:
            self._portal = config.get("iscsi_portal", "")
            self._target_iqn = config.get("iscsi_target_iqn", "")

        self._device_path: Path | None = None
        self._mount_dir: Path | None = None
        self._temp_dir_obj = None
        self._logged_in = False

    @property
    def protocol_name(self) -> str:
        return "iscsi"

    @property
    def mount_point(self) -> Path | None:
        return self._mount_dir

    def connect(self) -> None:
        """Discover targets, login, find device, format, and mount."""
        self._discover()
        self._login()
        time.sleep(2)  # Allow udev to create the device node
        self._device_path = self._find_device()
        if self._device_path:
            self._format_and_mount()

    def disconnect(self) -> None:
        """Unmount and iSCSI logout."""
        if self._mount_dir:
            subprocess.run(["umount", str(self._mount_dir)], capture_output=True)
        if self._temp_dir_obj:
            self._temp_dir_obj.cleanup()
        if self._logged_in:
            self._logout()
        self._device_path = None
        self._mount_dir = None
        self._logged_in = False

    def write_file(self, remote_path: str, data: bytes) -> None:
        path = self._resolve(remote_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def read_file(self, remote_path: str) -> bytes:
        return self._resolve(remote_path).read_bytes()

    def list_directory(self, remote_path: str) -> list[str]:
        return [p.name for p in self._resolve(remote_path).iterdir()]

    def delete_file(self, remote_path: str) -> None:
        self._resolve(remote_path).unlink(missing_ok=True)

    # ─── iSCSI internals ──────────────────────────────────────────────────────

    def _discover(self) -> None:
        result = subprocess.run(
            ["iscsiadm", "-m", "discovery", "-t", "st", "-p", self._portal],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            logger.warning("iSCSI discovery failed: %s", result.stderr)

    def _login(self) -> None:
        result = subprocess.run(
            ["iscsiadm", "-m", "node", "--targetname", self._target_iqn,
             "--portal", self._portal, "--login"],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"iSCSI login failed: {result.stderr}")
        self._logged_in = True
        logger.info("iSCSI logged in to %s via %s", self._target_iqn, self._portal)

    def _logout(self) -> None:
        subprocess.run(
            ["iscsiadm", "-m", "node", "--targetname", self._target_iqn,
             "--portal", self._portal, "--logout"],
            capture_output=True,
        )
        self._logged_in = False

    def _find_device(self) -> Path | None:
        """Find the block device created by iSCSI login via /sys/block."""
        result = subprocess.run(
            ["iscsiadm", "-m", "session", "-P", "3"],
            capture_output=True, text=True,
        )
        for line in result.stdout.splitlines():
            if "Attached scsi disk" in line:
                dev = line.split()[-1]
                path = Path(f"/dev/{dev}")
                if path.exists():
                    logger.info("iSCSI device: %s", path)
                    return path
        return None

    def _format_and_mount(self) -> None:
        """Format device with ext4 and mount to temp directory."""
        if not self._device_path:
            return
        subprocess.run(["mkfs.ext4", "-F", str(self._device_path)], capture_output=True)
        self._temp_dir_obj = tempfile.TemporaryDirectory(prefix="helix-iscsi-")
        self._mount_dir = Path(self._temp_dir_obj.name)
        result = subprocess.run(
            ["mount", str(self._device_path), str(self._mount_dir)],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"iSCSI device mount failed: {result.stderr}")

    def _resolve(self, remote_path: str) -> Path:
        if self._mount_dir is None:
            raise RuntimeError("iSCSIClient not mounted")
        return self._mount_dir / remote_path.lstrip("/")
