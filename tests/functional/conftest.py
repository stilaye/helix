"""
tests/functional/conftest.py — functional test fixtures.

Module-scoped: create shared resources once per test file, not per test.
This avoids the overhead of creating/deleting Cohesity views for every test.
"""

from __future__ import annotations

import uuid

import pytest


@pytest.fixture(scope="module")
def test_view_name() -> str:
    """Unique NAS view name for this test module."""
    return f"helix-test-{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="module")
def clean_protection_group(helios_client, helix_config):
    """
    Create a Cohesity protection group for testing backup/restore operations.
    Deleted with all snapshots at module teardown.
    """
    group_name = f"helix-pg-{uuid.uuid4().hex[:8]}"

    # Create protection group via Helios API
    group = helios_client.create_protection_group(
        name=group_name,
        cluster_id=helix_config.cluster_id,
    )

    yield group

    # Cleanup: delete protection group and all its snapshots
    try:
        helios_client.delete_protection_group(
            group_id=group.id,
            cluster_id=helix_config.cluster_id,
            delete_snapshots=True,
        )
    except Exception as e:
        pytest.warns(f"Could not delete protection group {group_name}: {e}")
