"""
ProtocolClient Abstract Base Class.

All four protocol clients (SMB, NFS, S3, iSCSI) implement this ABC.
This enables the pytest parametrize pattern:

    @pytest.mark.parametrize("protocol", ["smb", "nfs", "s3", "iscsi"])
    def test_backup_restore(protocol_client, protocol):
        ...

Adding a 5th protocol requires only a new concrete class — zero changes to tests.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from helix.api.models.storage import ShareConfig


class ProtocolClient(ABC):
    """
    Abstract base class for all storage protocol clients.
    Context manager: connect on __enter__, disconnect on __exit__ (always runs).
    """

    @abstractmethod
    def connect(self) -> None:
        """Establish connection/mount. Called by __enter__."""

    @abstractmethod
    def disconnect(self) -> None:
        """Tear down connection/unmount. Called by __exit__ (guaranteed)."""

    @abstractmethod
    def write_file(self, remote_path: str, data: bytes) -> None:
        """Write bytes to a file at remote_path."""

    @abstractmethod
    def read_file(self, remote_path: str) -> bytes:
        """Read and return file contents."""

    @abstractmethod
    def list_directory(self, remote_path: str) -> list[str]:
        """List file/directory names under remote_path."""

    @abstractmethod
    def delete_file(self, remote_path: str) -> None:
        """Delete a file."""

    def mkdir(self, remote_path: str) -> None:
        """Create a directory (optional override)."""
        raise NotImplementedError

    def exists(self, remote_path: str) -> bool:
        """Check if a path exists (optional override)."""
        raise NotImplementedError

    @property
    @abstractmethod
    def protocol_name(self) -> str:
        """Protocol identifier string, e.g. 'smb', 'nfs', 's3', 'iscsi'."""

    @property
    def mount_point(self) -> Path | None:
        """Local mount point for filesystem-backed protocols (SMB/NFS/iSCSI)."""
        return None

    def __enter__(self) -> "ProtocolClient":
        self.connect()
        return self

    def __exit__(self, *_: Any) -> None:
        self.disconnect()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(protocol={self.protocol_name})"


class ProtocolClientFactory:
    """Factory that creates the right ProtocolClient subclass from a string name."""

    @staticmethod
    def create(protocol: str, config: Any) -> ProtocolClient:
        """
        Args:
            protocol: One of 'smb', 'nfs', 's3', 'iscsi'.
            config: HelixConfig or dict with connection parameters.

        Returns:
            Configured ProtocolClient instance (not yet connected).
        """
        from helix.protocols.smb import SMBClient
        from helix.protocols.nfs import NFSClient
        from helix.protocols.s3 import S3Client
        from helix.protocols.iscsi import iSCSIClient

        mapping = {
            "smb": SMBClient,
            "nfs": NFSClient,
            "s3": S3Client,
            "iscsi": iSCSIClient,
        }
        cls = mapping.get(protocol.lower())
        if cls is None:
            raise ValueError(f"Unknown protocol '{protocol}'. Supported: {list(mapping)}")
        return cls(config)
