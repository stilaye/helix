"""
Demo: protocol tests — write / read / delete / checksum across SMB, NFS, S3.

Each test in this file runs 3 times (once per protocol) via the parametrized
protocol_client fixture in conftest.py. The interviewer sees:

  PASSED tests/demo/test_protocols.py::TestProtocolIO::test_write_read[SMB]
  PASSED tests/demo/test_protocols.py::TestProtocolIO::test_write_read[NFS]
  PASSED tests/demo/test_protocols.py::TestProtocolIO::test_write_read[S3]
  ...

This is the core demo of HELIX's parametrized protocol abstraction — one test
function exercises all storage protocols without any code duplication.

Run:  pytest tests/demo/test_protocols.py -v
"""

from __future__ import annotations

import hashlib
import os
import threading
import uuid

import pytest


pytestmark = [pytest.mark.functional, pytest.mark.regression]

TEST_DATA = b"HELIX protocol test payload\n" * 512   # ~14 KB
TEST_HASH = hashlib.sha256(TEST_DATA).hexdigest()


class TestProtocolIO:

    def test_write_read_checksum(self, protocol_client):
        """
        Core data integrity: write bytes, read back, verify sha256 checksum.
        Runs once per protocol (SMB / NFS / S3).
        """
        path = f"/helix-demo/{uuid.uuid4().hex}.bin"
        protocol_client.write_file(path, TEST_DATA)

        read_back = protocol_client.read_file(path)
        actual_hash = hashlib.sha256(read_back).hexdigest()

        assert actual_hash == TEST_HASH, (
            f"[{protocol_client}] Checksum mismatch — data corrupted in transit\n"
            f"  Expected: {TEST_HASH}\n"
            f"  Got:      {actual_hash}"
        )
        protocol_client.delete_file(path)

    def test_delete_removes_file(self, protocol_client):
        """After delete, reading the file must raise (no silent data leakage)."""
        path = f"/helix-demo/delete_test_{uuid.uuid4().hex}.txt"
        protocol_client.write_file(path, b"ephemeral")
        protocol_client.delete_file(path)

        with pytest.raises((FileNotFoundError, Exception)):
            protocol_client.read_file(path)

    def test_list_directory(self, protocol_client):
        """Directory listing must include files we wrote."""
        prefix = f"/helix-demo/list-test-{uuid.uuid4().hex[:6]}"
        paths = [f"{prefix}/file_{i}.txt" for i in range(3)]

        for p in paths:
            protocol_client.write_file(p, b"list test")

        listed = protocol_client.list_directory(f"{prefix}/")
        assert all(p in listed for p in paths), (
            f"[{protocol_client}] Not all files appeared in directory listing.\n"
            f"  Expected: {paths}\n"
            f"  Listed:   {listed}"
        )
        for p in paths:
            protocol_client.delete_file(p)

    def test_overwrite_updates_content(self, protocol_client):
        """Writing to an existing path must overwrite (no phantom old data)."""
        path = f"/helix-demo/overwrite_{uuid.uuid4().hex}.txt"
        protocol_client.write_file(path, b"version-1")
        protocol_client.write_file(path, b"version-2")

        content = protocol_client.read_file(path)
        assert content == b"version-2", (
            f"[{protocol_client}] Overwrite failed: got {content!r}"
        )
        protocol_client.delete_file(path)

    def test_large_file_integrity(self, protocol_client):
        """1 MB write → read, checksum must match (catches partial writes)."""
        large_data = os.urandom(1 * 1024 * 1024)
        expected = hashlib.sha256(large_data).hexdigest()
        path = f"/helix-demo/large_{uuid.uuid4().hex}.bin"

        protocol_client.write_file(path, large_data)
        read_data = protocol_client.read_file(path)
        actual = hashlib.sha256(read_data).hexdigest()

        assert actual == expected, f"[{protocol_client}] 1 MB checksum mismatch"
        protocol_client.delete_file(path)

    def test_concurrent_writes_no_corruption(self, protocol_client):
        """
        4 threads write to distinct paths simultaneously.
        Verifies the protocol client handles concurrent access without corruption.
        Models multi-VM backup agent workload.
        """
        errors = []
        results = {}

        def write_verify(tid: int) -> None:
            data = os.urandom(64 * 1024)   # 64 KB
            expected = hashlib.sha256(data).hexdigest()
            path = f"/helix-demo/concurrent_{tid}_{uuid.uuid4().hex[:6]}.bin"
            try:
                protocol_client.write_file(path, data)
                read_back = protocol_client.read_file(path)
                actual = hashlib.sha256(read_back).hexdigest()
                if actual != expected:
                    errors.append(f"Thread {tid}: checksum mismatch")
                else:
                    results[tid] = "OK"
                protocol_client.delete_file(path)
            except Exception as e:
                errors.append(f"Thread {tid}: {e}")

        threads = [threading.Thread(target=write_verify, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, (
            f"[{protocol_client}] Concurrent write errors:\n" + "\n".join(errors)
        )
        assert len(results) == 4, (
            f"[{protocol_client}] Only {len(results)}/4 concurrent writes succeeded"
        )
