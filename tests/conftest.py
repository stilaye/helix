"""
tests/conftest.py — suite-level fixtures.

Key fixture: protocol_client (parametrized across all 4 storage protocols).
One test function that uses this fixture runs 4 times automatically —
once for SMB, NFS, S3, and iSCSI — with zero code duplication.

Adding a 5th protocol (HDFS, CIFS2, etc.) requires changing ONLY this fixture.
All existing tests pick it up automatically.
"""

from __future__ import annotations

import pytest

from helix.protocols.base import ProtocolClient, ProtocolClientFactory


@pytest.fixture(
    params=["smb", "nfs", "s3", "iscsi"],
    ids=["SMB", "NFS", "S3", "iSCSI"],
)
def protocol_client(request: pytest.FixtureRequest, helix_config) -> ProtocolClient:
    """
    Parametrized fixture: yields one connected ProtocolClient per storage protocol.

    Each test using this fixture runs 4 times:
      PASS[SMB] PASS[NFS] PASS[S3] PASS[iSCSI]

    Protocols are skipped gracefully if their required config is missing.
    """
    protocol = request.param
    _check_protocol_prereqs(protocol, helix_config)

    with ProtocolClientFactory.create(protocol, helix_config) as client:
        yield client


@pytest.fixture(params=["smb", "nfs"], ids=["SMB", "NFS"])
def nas_protocol_client(request: pytest.FixtureRequest, helix_config) -> ProtocolClient:
    """NAS-only parametrized fixture (SMB + NFS) for NAS-specific tests."""
    protocol = request.param
    _check_protocol_prereqs(protocol, helix_config)
    with ProtocolClientFactory.create(protocol, helix_config) as client:
        yield client


def _check_protocol_prereqs(protocol: str, config) -> None:
    """Skip a protocol test if required configuration is absent."""
    if protocol == "smb":
        if not config.smb_share or not config.cluster_ip:
            pytest.skip("SMB config incomplete (SMB_SHARE, CLUSTER_IP)")
    elif protocol == "nfs":
        if not config.nfs_export or not config.cluster_ip:
            pytest.skip("NFS config incomplete (NFS_EXPORT, CLUSTER_IP)")
    elif protocol == "s3":
        if not config.s3_access_key or not config.s3_secret_key:
            pytest.skip("S3 config incomplete (S3_ACCESS_KEY, S3_SECRET_KEY)")
    elif protocol == "iscsi":
        if not config.iscsi_target:
            pytest.skip("iSCSI config incomplete (ISCSI_TARGET)")
