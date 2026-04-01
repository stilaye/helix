"""
Smoke: basic protocol connectivity — one-file write/read/delete per protocol.

Uses the parametrized protocol_client fixture from tests/conftest.py.
Each test runs 4 times: [SMB] [NFS] [S3] [iSCSI].

Smoke-level only: verifies the protocol stack is wired up, not data integrity.
Detailed functional tests are in tests/functional/.
"""

import hashlib
import os
import uuid

import allure
import pytest

pytestmark = [pytest.mark.smoke]

TEST_DATA = b"HELIX smoke test data\n" * 64   # ~1.3 KB
TEST_DATA_HASH = hashlib.sha256(TEST_DATA).hexdigest()


@allure.suite("Smoke")
@allure.feature("Protocol Connectivity")
class TestProtocolsBasic:

    @allure.title("Write and read back a small file via {protocol_client}")
    def test_write_read(self, protocol_client):
        """Verify basic write → read round-trip for the protocol."""
        remote_path = f"/helix-smoke/{uuid.uuid4().hex}.txt"

        with allure.step(f"Write {len(TEST_DATA)} bytes to {remote_path}"):
            protocol_client.write_file(remote_path, TEST_DATA)

        with allure.step("Read back and verify content"):
            read_back = protocol_client.read_file(remote_path)
            assert read_back == TEST_DATA, (
                f"Data mismatch: expected sha256={TEST_DATA_HASH}, "
                f"got sha256={hashlib.sha256(read_back).hexdigest()}"
            )

        with allure.step("Delete test file"):
            protocol_client.delete_file(remote_path)

    @allure.title("List directory returns entries via {protocol_client}")
    def test_list_directory(self, protocol_client):
        """Verify directory listing returns a non-error response."""
        with allure.step("List root/export directory"):
            entries = protocol_client.list_directory("/")
        # May be empty, but must not raise
        assert isinstance(entries, list)

    @allure.title("Protocol client reconnects cleanly after disconnect")
    def test_reconnect(self, helix_config, request):
        """Verify connect → disconnect → connect cycle works."""
        protocol = request.node.callspec.params.get("protocol_client", "unknown")
        from helix.protocols.base import ProtocolClientFactory

        with allure.step("First connection"):
            client = ProtocolClientFactory.create(protocol, helix_config)
            client.connect()

        with allure.step("Disconnect"):
            client.disconnect()

        with allure.step("Reconnect"):
            client.connect()
            entries = client.list_directory("/")
            assert isinstance(entries, list)
            client.disconnect()
