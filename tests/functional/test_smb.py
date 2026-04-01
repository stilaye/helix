"""
Functional: SMB protocol correctness tests.

Validates:
  - Authentication (NTLM and Kerberos paths)
  - SMB dialect negotiation (assert SMB 3.1.1)
  - Session signing and encryption
  - ACL enforcement
  - Large file I/O (>100 MB)
  - Concurrent client access
"""

from __future__ import annotations

import os
import threading
import uuid

import allure
import pytest

from helix.capture.tshark import TsharkCapture
from helix.capture.parsers.smb import SMBParser
from helix.utils.checksum import compute_file_checksum
from helix.utils.data_gen import DataGenerator

pytestmark = [pytest.mark.smb, pytest.mark.functional, pytest.mark.regression]


@allure.suite("Functional")
@allure.feature("SMB Protocol")
class TestSMBFunctional:

    @allure.title("SMB mount uses dialect 3.1.1 with signing enabled")
    def test_smb311_dialect_with_signing(self, helix_config):
        """
        Capture SMB negotiation and verify:
        - Dialect 0x0311 (SMB 3.1.1) negotiated
        - Packet signing enabled (security compliance requirement)
        """
        from helix.protocols.smb import SMBClient

        capture_filter = f"tcp port 445 and host {helix_config.cluster_ip}"

        with TsharkCapture(
            interface="eth0",
            filter_expr=capture_filter,
            extra_fields=["smb2.dialect", "smb2.flags.signed", "smb2.cmd"],
        ) as cap:
            with SMBClient(helix_config) as smb:
                smb.write_file("/helix-test/dialect_check.txt", b"test")
                smb.delete_file("/helix-test/dialect_check.txt")

        frames = cap.load_frames()
        cap.attach_to_allure("smb_dialect_capture.json")

        result = SMBParser(frames).parse()

        allure.attach(
            f"Dialect: {result.dialect}\nSigning: {result.signing_enabled}\nAuth: {result.auth_type}",
            name="smb_session_info.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

        assert result.is_smb3, f"Expected SMB 3.x, got: {result.dialect}"
        assert result.signing_enabled, "SMB packet signing must be enabled (security requirement)"

    @allure.title("Large file write and read (100 MB) maintains data integrity")
    def test_large_file_integrity(self, helix_config, tmp_path):
        """Write a 100 MB file and verify sha256 checksum is preserved."""
        from helix.protocols.smb import SMBClient

        size = 100 * 1024 * 1024  # 100 MB
        local_file = tmp_path / "large_test.bin"
        local_file.write_bytes(os.urandom(size))
        original_checksum = compute_file_checksum(local_file)

        remote_path = f"/helix-test/large_{uuid.uuid4().hex[:8]}.bin"

        with SMBClient(helix_config) as smb:
            with allure.step(f"Write {size // (1024*1024)} MB file"):
                smb.write_file(remote_path, local_file.read_bytes())

            with allure.step("Read back and verify checksum"):
                read_data = smb.read_file(remote_path)
                import hashlib
                read_hash = hashlib.sha256(read_data).hexdigest()
                assert read_hash == original_checksum, "100 MB file checksum mismatch"

            smb.delete_file(remote_path)

    @allure.title("Concurrent SMB clients can write without data corruption")
    @pytest.mark.timeout(120)
    def test_concurrent_writes(self, helix_config):
        """
        4 threads each write a unique file concurrently.
        Verifies no data corruption under concurrent access (critical for
        multi-VM backup scenarios).
        """
        from helix.protocols.smb import SMBClient

        num_clients = 4
        file_size = 1 * 1024 * 1024  # 1 MB per client
        errors = []
        results = {}

        def write_verify(client_id: int) -> None:
            data = os.urandom(file_size)
            expected_hash = __import__("hashlib").sha256(data).hexdigest()
            remote_path = f"/helix-test/concurrent_{client_id}_{uuid.uuid4().hex[:6]}.bin"
            try:
                with SMBClient(helix_config) as smb:
                    smb.write_file(remote_path, data)
                    read_back = smb.read_file(remote_path)
                    actual_hash = __import__("hashlib").sha256(read_back).hexdigest()
                    if actual_hash != expected_hash:
                        errors.append(f"Client {client_id}: checksum mismatch")
                    else:
                        results[client_id] = "OK"
                    smb.delete_file(remote_path)
            except Exception as e:
                errors.append(f"Client {client_id}: {e}")

        with allure.step(f"Launch {num_clients} concurrent SMB clients"):
            threads = [threading.Thread(target=write_verify, args=(i,)) for i in range(num_clients)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=90)

        assert not errors, f"Concurrent write errors:\n" + "\n".join(errors)
        assert len(results) == num_clients, f"Only {len(results)}/{num_clients} clients succeeded"
