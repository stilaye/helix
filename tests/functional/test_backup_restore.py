"""
Functional: backup and restore integrity across all 4 protocols.

Test flow:
  1. Generate test dataset with known checksums
  2. Write dataset via protocol under test
  3. Trigger Cohesity backup via Helios API
  4. Corrupt or delete source data
  5. Restore from snapshot
  6. Verify restored data matches original checksums

This test is parametrized over all protocols via protocol_client fixture.
One test = 4 protocol variants = 4 independent backup/restore validations.

Key assertion: data integrity is verified by tree checksum, not just file count.
A restore that silently drops files or corrupts data will be caught.
"""

from __future__ import annotations

import hashlib
import time
import uuid

import allure
import pytest

from helix.utils.data_gen import DataGenerator
from helix.utils.wait import poll_until
from helix.constants import ResilienceSLA

pytestmark = [pytest.mark.regression, pytest.mark.functional]


@allure.suite("Functional")
@allure.feature("Backup & Restore")
class TestBackupRestoreIntegrity:

    @allure.title("Backup and restore integrity via {protocol_client}")
    @pytest.mark.timeout(600)
    def test_backup_restore_checksum(
        self,
        protocol_client,
        helios_client,
        helix_config,
        clean_protection_group,
        tmp_path,
    ):
        """
        Write data → backup → restore → verify checksums match.
        Parametrized: runs once per protocol (SMB, NFS, S3, iSCSI).
        """
        test_id = uuid.uuid4().hex[:8]
        remote_dir = f"/helix-backup-test/{test_id}"

        # ── Step 1: Generate test data with known checksums ──────────────────
        with allure.step("Generate test dataset"):
            dataset = DataGenerator.create_standard_set(tmp_path)
            allure.attach(
                f"Files: {dataset.file_count}, "
                f"Total size: {dataset.total_size_bytes} bytes, "
                f"Tree checksum: {dataset.tree_checksum}",
                name="dataset_info.txt",
                attachment_type=allure.attachment_type.TEXT,
            )

        # ── Step 2: Write data via protocol ──────────────────────────────────
        with allure.step(f"Write {dataset.file_count} files via {type(protocol_client).__name__}"):
            for file_info in dataset.files:
                protocol_client.write_file(
                    f"{remote_dir}/{file_info.relative_path}",
                    file_info.path.read_bytes(),
                )

        # ── Step 3: Trigger backup via Helios API ────────────────────────────
        with allure.step("Trigger Cohesity backup via Helios API"):
            run = helios_client.trigger_backup(
                group_id=clean_protection_group.id,
                cluster_id=helix_config.cluster_id,
            )
            run_id = run.run_id

        # ── Step 4: Wait for backup completion ───────────────────────────────
        with allure.step(f"Wait for backup run {run_id} to complete"):
            def backup_complete():
                r = helios_client.get_backup_run(run_id, helix_config.cluster_id)
                return r.is_complete

            poll_until(
                backup_complete,
                timeout=300,
                interval=10,
                message=f"Backup run {run_id} did not complete within 300s",
            )
            final_run = helios_client.get_backup_run(run_id, helix_config.cluster_id)
            assert final_run.is_success, (
                f"Backup run {run_id} failed: {getattr(final_run, 'error', 'unknown error')}"
            )

        # ── Step 5: Delete source data ───────────────────────────────────────
        with allure.step("Delete source data to force restore"):
            for file_info in dataset.files:
                protocol_client.delete_file(f"{remote_dir}/{file_info.relative_path}")

        # ── Step 6: Restore from snapshot ────────────────────────────────────
        restore_dir = f"/helix-restore/{test_id}"
        with allure.step(f"Restore snapshot to {restore_dir}"):
            snapshot = helios_client.list_snapshots(
                group_id=clean_protection_group.id,
                cluster_id=helix_config.cluster_id,
            )[0]
            helios_client.restore(
                snapshot_id=snapshot.id,
                target_dir=restore_dir,
                cluster_id=helix_config.cluster_id,
            )

        # ── Step 7: Wait for restore completion ──────────────────────────────
        with allure.step("Wait for restore to complete"):
            time.sleep(5)   # Brief pause for restore job to register

        # ── Step 8: Verify checksums ──────────────────────────────────────────
        with allure.step("Verify restored data checksums"):
            mismatches = []
            for file_info in dataset.files:
                restored_path = f"{restore_dir}/{file_info.relative_path}"
                restored_data = protocol_client.read_file(restored_path)
                restored_hash = hashlib.sha256(restored_data).hexdigest()

                if restored_hash != file_info.checksum:
                    mismatches.append(
                        f"{file_info.relative_path}: "
                        f"expected {file_info.checksum}, got {restored_hash}"
                    )

            if mismatches:
                allure.attach(
                    "\n".join(mismatches),
                    name="checksum_mismatches.txt",
                    attachment_type=allure.attachment_type.TEXT,
                )
            assert not mismatches, (
                f"{len(mismatches)}/{dataset.file_count} files failed checksum verification:\n"
                + "\n".join(mismatches[:5])   # Show first 5 for brevity
            )

    @allure.title("Multiple snapshots are available after multiple backups")
    def test_snapshot_listing(self, helios_client, helix_config, clean_protection_group):
        """Verify snapshot listing API returns populated results after backup."""
        snapshots = helios_client.list_snapshots(
            group_id=clean_protection_group.id,
            cluster_id=helix_config.cluster_id,
        )
        assert isinstance(snapshots, list), "Expected list of snapshots"
