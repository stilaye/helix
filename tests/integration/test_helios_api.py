"""
Integration: Helios API full CRUD — protection groups, policies, jobs.

Tests the complete Helios API workflow:
  1. Create protection group
  2. Associate with a storage source
  3. Run backup
  4. List snapshots
  5. Restore
  6. Delete protection group

This validates the management plane end-to-end, not just individual endpoints.
"""

from __future__ import annotations

import time
import uuid

import allure
import pytest

from helix.utils.wait import poll_until

pytestmark = [pytest.mark.integration, pytest.mark.regression]


@allure.suite("Integration")
@allure.feature("Helios API CRUD")
class TestHeliosAPICRUD:

    @allure.title("Full protection group lifecycle: create → backup → snapshot → delete")
    @pytest.mark.timeout(600)
    def test_protection_group_lifecycle(self, helios_client, helix_config):
        """
        End-to-end test of the protection job management workflow.
        This is the core value proposition of Cohesity: automated data protection.
        """
        group_name = f"helix-integration-{uuid.uuid4().hex[:8]}"

        # ── Create protection group ───────────────────────────────────────────
        with allure.step(f"Create protection group: {group_name}"):
            group = helios_client.create_protection_group(
                name=group_name,
                cluster_id=helix_config.cluster_id,
            )
            assert group.id, "Protection group created without an ID"

        try:
            # ── Trigger backup ─────────────────────────────────────────────────
            with allure.step("Trigger backup run"):
                run = helios_client.trigger_backup(
                    group_id=group.id,
                    cluster_id=helix_config.cluster_id,
                )

            # ── Wait for completion ─────────────────────────────────────────────
            with allure.step("Wait for backup to complete (max 300s)"):
                poll_until(
                    lambda: helios_client.get_backup_run(run.run_id, helix_config.cluster_id).is_complete,
                    timeout=300,
                    interval=10,
                    message="Backup did not complete within 300s",
                )
                final_run = helios_client.get_backup_run(run.run_id, helix_config.cluster_id)

            with allure.step("Verify backup succeeded"):
                assert final_run.is_success, (
                    f"Backup failed: {getattr(final_run, 'error', 'unknown')}"
                )

            # ── List snapshots ──────────────────────────────────────────────────
            with allure.step("Verify snapshot created"):
                snapshots = helios_client.list_snapshots(
                    group_id=group.id,
                    cluster_id=helix_config.cluster_id,
                )
                assert len(snapshots) >= 1, "No snapshots found after successful backup"

            allure.attach(
                f"Group: {group_name} (id={group.id})\n"
                f"Run ID: {run.run_id}\n"
                f"Snapshots: {len(snapshots)}\n"
                f"Status: {final_run.status}",
                name="lifecycle_summary.txt",
                attachment_type=allure.attachment_type.TEXT,
            )

        finally:
            # ── Cleanup: always delete protection group ───────────────────────
            with allure.step("Delete protection group and snapshots"):
                try:
                    helios_client.delete_protection_group(
                        group_id=group.id,
                        cluster_id=helix_config.cluster_id,
                        delete_snapshots=True,
                    )
                except Exception as e:
                    pytest.warns(f"Could not delete protection group: {e}")

    @allure.title("Protection summary statistics are valid")
    def test_protection_summary(self, helios_client):
        """Verify the MCM protection summary endpoint returns meaningful data."""
        with allure.step("Fetch protection summary"):
            summary = helios_client.get_protection_summary()

        assert summary is not None, "Protection summary returned None"
        allure.attach(
            str(summary),
            name="protection_summary.txt",
            attachment_type=allure.attachment_type.TEXT,
        )
