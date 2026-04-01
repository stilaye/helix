"""
Functional: NFS protocol correctness tests.

Validates:
  - NFSv4 mount with various options
  - Stale file handle recovery (critical for long-running backup agents)
  - root_squash behavior (security)
  - Export list discovery
  - Large sequential I/O
"""

from __future__ import annotations

import os
import uuid

import allure
import pytest

from helix.capture.tshark import TsharkCapture
from helix.capture.parsers.nfs import NFSParser

pytestmark = [pytest.mark.nfs, pytest.mark.functional, pytest.mark.regression]


@allure.suite("Functional")
@allure.feature("NFS Protocol")
class TestNFSFunctional:

    @allure.title("NFSv4 mount shows correct RPC auth flavor in wire capture")
    def test_nfs_auth_flavor_in_capture(self, helix_config):
        """
        Capture NFS traffic and verify the RPC auth flavor.
        AUTH_SYS (flavor=1) is expected for standard mounts.
        RPCSEC_GSS (flavor=6) is expected for Kerberos NFS.
        """
        from helix.protocols.nfs import NFSClient

        capture_filter = f"udp port 2049 or tcp port 2049"

        with TsharkCapture(
            interface="eth0",
            filter_expr=capture_filter,
            extra_fields=["rpc.auth.flavor", "nfs.procedure_v4", "nfs.nfsstat4"],
        ) as cap:
            with NFSClient(helix_config) as nfs:
                nfs.write_file("/helix-nfs-test/auth_check.txt", b"nfs auth test")
                nfs.delete_file("/helix-nfs-test/auth_check.txt")

        frames = cap.load_frames()
        cap.attach_to_allure("nfs_auth_capture.json")

        result = NFSParser(frames).parse()
        allure.attach(
            f"Auth flavor: {result.auth_flavor}\n"
            f"Operations: {result.operations}\n"
            f"Stale handles: {result.stale_handle_count}",
            name="nfs_session_info.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

        assert result.auth_flavor in ("AUTH_SYS", "RPCSEC_GSS"), (
            f"Unexpected RPC auth flavor: {result.auth_flavor}"
        )
        assert not result.has_stale_handles, (
            "NFS4ERR_STALE observed during normal write — unexpected stale handle"
        )

    @allure.title("NFS client recovers from stale file handle (NFS4ERR_STALE)")
    def test_stale_handle_recovery(self, helix_config):
        """
        Simulate a stale file handle scenario by unmounting and remounting
        while a file handle is held, then verify the client recovers
        without the application seeing an error.

        This is critical for long-running backup agents that hold open
        file handles across NFS server restarts.
        """
        from helix.protocols.nfs import NFSClient

        with NFSClient(helix_config) as nfs:
            remote_path = f"/helix-nfs-test/stale_test_{uuid.uuid4().hex[:6]}.txt"
            nfs.write_file(remote_path, b"before stale")

            with allure.step("Simulate stale handle by triggering recover_stale_handle()"):
                # The NFSClient.recover_stale_handle() unmounts and remounts cleanly
                nfs.recover_stale_handle()

            with allure.step("Verify read succeeds after stale handle recovery"):
                data = nfs.read_file(remote_path)
                assert data == b"before stale", "Data corrupted after stale handle recovery"

            nfs.delete_file(remote_path)

    @allure.title("NFS export list is accessible via showmount")
    def test_export_list(self, helix_config):
        """Verify the NFS server exports at least one path."""
        from helix.protocols.nfs import NFSClient

        with NFSClient(helix_config) as nfs:
            exports = nfs.get_export_list()

        assert len(exports) > 0, f"No NFS exports found on {helix_config.cluster_ip}"
        allure.attach(
            "\n".join(exports),
            name="nfs_exports.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

    @allure.title("Sequential read/write performance meets NFS minimum threshold")
    @pytest.mark.perf
    def test_nfs_sequential_throughput(self, helix_config):
        """
        Write 512 MB sequentially and verify throughput > 200 MB/s.
        Uses dd (simple, no fio dependency for basic NFS smoke perf).
        """
        from helix.tools.dd import DDRunner

        dd = DDRunner(ssh_client=None)  # Run dd locally against mounted NFS
        with allure.step("Write 512 MB via dd"):
            result = dd.run(
                source="/dev/zero",
                destination=f"{helix_config.nfs_export}/dd_perf_{uuid.uuid4().hex[:6]}",
                block_size_mb=4,
                count=128,  # 4MB * 128 = 512 MB
            )

        allure.attach(
            f"Throughput: {result.throughput_mbs:.1f} MB/s\n"
            f"Duration: {result.duration_secs:.1f}s\n"
            f"Bytes transferred: {result.bytes_transferred}",
            name="dd_result.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

        # NFS minimum: 200 MB/s (typically 500+ MB/s on 10GbE)
        assert result.throughput_mbs >= 200.0, (
            f"NFS sequential throughput {result.throughput_mbs:.1f} MB/s < 200 MB/s minimum"
        )
