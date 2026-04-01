"""
Demo: smoke tests running against mock cluster.

Runs the EXACT same assertions as tests/smoke/test_cluster_health.py and
tests/smoke/test_api_reachable.py — the only difference is the fixture layer
(mock vs real Helios API).

Run during interview:  pytest tests/demo/test_smoke.py -v
"""

import pytest


pytestmark = [pytest.mark.smoke]


class TestClusterHealth:

    def test_all_nodes_healthy(self, helios_client):
        """All 3 cluster nodes must report kHealthy status."""
        info = helios_client.get_cluster_info()
        unhealthy = [n for n in info.nodes if not n.is_healthy]
        assert not unhealthy, (
            f"{len(unhealthy)}/{info.node_count} nodes unhealthy: "
            + ", ".join(f"{n.id}({n.status})" for n in unhealthy)
        )

    def test_quorum_ok(self, helios_client):
        """Cluster must have quorum (majority of nodes healthy)."""
        info = helios_client.get_cluster_info()
        assert info.quorum_ok, (
            f"No quorum: {info.healthy_node_count}/{info.node_count} nodes healthy"
        )

    def test_leader_elected(self, helios_client):
        """Exactly one node must hold the kLeader role."""
        info = helios_client.get_cluster_info()
        leaders = [n for n in info.nodes if n.is_leader]
        assert len(leaders) == 1, f"Expected 1 leader, found {len(leaders)}"

    def test_consistent_software_version(self, helios_client):
        """All nodes must run the same software version (catches failed NDU)."""
        info = helios_client.get_cluster_info()
        versions = {n.id: n.software_version for n in info.nodes
                    if n.software_version and n.software_version != "unknown"}
        unique = set(versions.values())
        assert len(unique) <= 1, f"Version mismatch across nodes: {versions}"


class TestHeliosAPI:

    def test_api_ping(self, helios_client):
        """Helios API authentication and ping must succeed."""
        result = helios_client.ping()
        assert result, "Helios ping returned empty response"

    def test_list_clusters(self, helios_client):
        """MCM must return at least one registered cluster."""
        clusters = helios_client.list_clusters()
        assert len(clusters) >= 1, "No clusters registered in Helios MCM"

    def test_list_protection_jobs(self, helios_client, helix_config):
        """Protection jobs endpoint must return a valid list."""
        jobs = helios_client.list_protection_jobs()
        assert isinstance(jobs, list)
        # Verify Pydantic model properties work
        for job in jobs:
            assert hasattr(job, "is_running")

    def test_alerts_endpoint(self, helios_client):
        """Alerts endpoint must return valid Alert model objects."""
        alerts = helios_client.list_alerts()
        assert isinstance(alerts, list)
        # Each alert must have the is_critical property (from Pydantic model)
        for alert in alerts:
            _ = alert.is_critical   # property access, must not raise
