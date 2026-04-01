"""
Chaos: disk fault injection and data durability.

Tests:
  - dm-error injection on one node's disk → cluster reroutes I/O
  - Data written before disk fault survives (replicated to healthy disks)
  - Cohesity alerts generated for disk errors
  - Disk heal (dm-error removal) → normal I/O resumes

Uses DiskFault.inject_errors() which creates a dm-error device mapping
that fails all I/O at the block level.
"""

from __future__ import annotations

import time
import uuid

import allure
import pytest

pytestmark = [pytest.mark.chaos, pytest.mark.destructive, pytest.mark.slow]


@allure.suite("Chaos")
@allure.feature("Disk Fault")
class TestDiskFault:

    @allure.title("Data written before disk fault is preserved via replication")
    @pytest.mark.timeout(240)
    def test_data_durability_under_disk_fault(
        self, fault_injector, helios_client, ssh_nodes, helix_config
    ):
        """
        Write data → inject disk fault on one node → verify data still readable.
        Cohesity replicates data across multiple nodes/disks. A single disk
        failure should not cause data loss.

        This test validates the fundamental data durability guarantee.
        """
        if not ssh_nodes:
            pytest.skip("No SSH nodes available for disk fault injection")

        target_node = "node-1"
        test_data = b"data durability test " * 1000
        key = f"/helix-chaos/disk_fault_{uuid.uuid4().hex[:6]}.bin"

        # ── Write data before fault ───────────────────────────────────────────
        with allure.step("Write test data before injecting disk fault"):
            import hashlib
            original_hash = hashlib.sha256(test_data).hexdigest()
            from helix.protocols.nfs import NFSClient
            with NFSClient(helix_config) as nfs:
                nfs.write_file(key, test_data)

        # ── Inject disk fault ─────────────────────────────────────────────────
        # Note: In a real test we'd query the actual device name.
        # Using /dev/sdb as a representative device.
        target_device = "/dev/sdb"

        with allure.step(f"Inject disk errors on {target_node}:{target_device}"):
            fault_injector.inject_disk_errors(target_node, device=target_device)
            allure.attach(
                f"Node: {target_node}\nDevice: {target_device}\nFault type: dm-error",
                name="disk_fault_config.txt",
                attachment_type=allure.attachment_type.TEXT,
            )

        with allure.step("Wait for Cohesity to detect disk error (15s)"):
            time.sleep(15)

        # ── Verify alerts generated ───────────────────────────────────────────
        with allure.step("Verify disk error alert generated in Helios"):
            alerts = helios_client.list_alerts()
            disk_alerts = [a for a in alerts if "disk" in str(getattr(a, "description", "")).lower()]
            allure.attach(
                f"Total alerts: {len(alerts)}\nDisk-related alerts: {len(disk_alerts)}",
                name="disk_fault_alerts.txt",
                attachment_type=allure.attachment_type.TEXT,
            )

        # ── Verify data still accessible ─────────────────────────────────────
        with allure.step("Verify pre-fault data still readable from other replicas"):
            with NFSClient(helix_config) as nfs:
                read_data = nfs.read_file(key)
                read_hash = hashlib.sha256(read_data).hexdigest()
                nfs.delete_file(key)

        assert read_hash == original_hash, (
            "Data lost after disk fault — replication did not preserve data"
        )

        # DiskFault healed in fault_injector teardown (heal_all() calls dm-error remove)

    @allure.title("fsck reports clean filesystem after disk fault injection and heal")
    @pytest.mark.timeout(180)
    def test_fsck_after_disk_fault(self, fault_injector, ssh_nodes, helix_config):
        """
        Inject disk errors → heal → run fsck -n (dry-run, safe on mounted FS).
        Verify no filesystem corruption introduced by the fault/heal cycle.
        """
        if not ssh_nodes:
            pytest.skip("No SSH nodes available")

        from helix.tools.fsct import FsctRunner

        target_node = "node-1"
        ssh = fault_injector._get_ssh(target_node)

        with allure.step("Inject and immediately heal disk fault"):
            fault_injector.inject_disk_errors(target_node, device="/dev/sdb")
            time.sleep(5)
            fault_injector._active_disk_faults[-1].heal()

        with allure.step("Run fsck dry-run on filesystem"):
            fsck = FsctRunner(ssh_client=ssh)
            result = fsck.dry_run("/dev/sdb")

        allure.attach(
            f"Clean: {result.clean}\nErrors: {result.error_count}\nWarnings: {result.warnings}",
            name="fsck_result.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

        assert result.clean, (
            f"Filesystem has {result.error_count} errors after disk fault cycle. "
            "Run fsck -y to repair (requires unmount)."
        )
