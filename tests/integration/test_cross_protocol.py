"""
Integration: cross-protocol data access — write via one protocol, read via another.

These tests validate Cohesity's unified namespace:
  - Data written via NFS is accessible via SMB and S3
  - Data written via S3 is accessible via NFS
  - Checksums match across all protocol access paths

This is a key differentiator of Cohesity's SmartFiles platform — one data store,
multiple protocol front-ends with consistent view.
"""

from __future__ import annotations

import hashlib
import os
import uuid

import allure
import pytest

from helix.utils.checksum import compute_file_checksum

pytestmark = [pytest.mark.integration, pytest.mark.regression]


@allure.suite("Integration")
@allure.feature("Cross-Protocol Access")
class TestCrossProtocolAccess:

    @allure.title("Write via NFS, read back via SMB — checksum must match")
    @pytest.mark.timeout(120)
    def test_nfs_write_smb_read(self, helix_config):
        """
        Write a file via NFS, then read it back via SMB.
        Verifies that the unified namespace exposes the same data
        regardless of which protocol front-end is used.
        """
        from helix.protocols.nfs import NFSClient
        from helix.protocols.smb import SMBClient

        test_data = os.urandom(1 * 1024 * 1024)  # 1 MB
        original_hash = hashlib.sha256(test_data).hexdigest()
        filename = f"cross_proto_{uuid.uuid4().hex[:8]}.bin"
        nfs_path = f"/helix-xproto/{filename}"
        smb_path = f"/helix-xproto/{filename}"  # Same logical path, different protocol

        with allure.step("Write 1 MB file via NFS"):
            with NFSClient(helix_config) as nfs:
                nfs.write_file(nfs_path, test_data)

        with allure.step("Read back the same file via SMB"):
            with SMBClient(helix_config) as smb:
                read_data = smb.read_file(smb_path)

        read_hash = hashlib.sha256(read_data).hexdigest()

        allure.attach(
            f"Original sha256: {original_hash}\n"
            f"Read via SMB sha256: {read_hash}\n"
            f"Match: {original_hash == read_hash}",
            name="cross_protocol_nfs_smb.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

        assert read_hash == original_hash, (
            "NFS write / SMB read checksum mismatch — unified namespace inconsistency"
        )

        # Cleanup via NFS
        with NFSClient(helix_config) as nfs:
            nfs.delete_file(nfs_path)

    @allure.title("Write via NFS, read via S3 — data consistent across protocols")
    @pytest.mark.timeout(120)
    def test_nfs_write_s3_read(self, helix_config):
        """
        Write via NFS, read via S3 (object storage view of the same data).
        Tests the NAS-to-S3 data path consistency.
        """
        from helix.protocols.nfs import NFSClient
        from helix.protocols.s3 import S3Client

        test_data = os.urandom(512 * 1024)  # 512 KB
        original_hash = hashlib.sha256(test_data).hexdigest()
        filename = f"nfs_to_s3_{uuid.uuid4().hex[:8]}.bin"
        nfs_path = f"/helix-xproto/{filename}"
        s3_key = f"helix-xproto/{filename}"

        with allure.step("Write via NFS"):
            with NFSClient(helix_config) as nfs:
                nfs.write_file(nfs_path, test_data)

        with allure.step("Read back via S3"):
            with S3Client(helix_config) as s3:
                read_data = s3.read_file(s3_key)

        read_hash = hashlib.sha256(read_data).hexdigest()
        assert read_hash == original_hash, "NFS write / S3 read checksum mismatch"

        with NFSClient(helix_config) as nfs:
            nfs.delete_file(nfs_path)

    @allure.title("Write via S3, read via NFS — object visible in NAS namespace")
    @pytest.mark.timeout(120)
    def test_s3_write_nfs_read(self, helix_config):
        """Write an S3 object and verify it's accessible as a file via NFS."""
        from helix.protocols.s3 import S3Client
        from helix.protocols.nfs import NFSClient

        test_data = os.urandom(256 * 1024)  # 256 KB
        original_hash = hashlib.sha256(test_data).hexdigest()
        filename = f"s3_to_nfs_{uuid.uuid4().hex[:8]}.bin"
        s3_key = f"helix-xproto/{filename}"
        nfs_path = f"/helix-xproto/{filename}"

        with allure.step("Write via S3"):
            with S3Client(helix_config) as s3:
                s3.write_file(s3_key, test_data)

        with allure.step("Read via NFS"):
            with NFSClient(helix_config) as nfs:
                read_data = nfs.read_file(nfs_path)

        read_hash = hashlib.sha256(read_data).hexdigest()
        assert read_hash == original_hash, "S3 write / NFS read checksum mismatch"

        with S3Client(helix_config) as s3:
            s3.delete_file(s3_key)
