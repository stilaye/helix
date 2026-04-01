"""
Smoke: cluster health — verify quorum, all nodes up, leader elected.

These tests run first on every PR. If any fail, the entire suite is skipped.
Target: complete in < 60 seconds.
"""

import pytest
import allure

pytestmark = [pytest.mark.smoke]


@allure.suite("Smoke")
@allure.feature("Cluster Health")
class TestClusterHealth:

    @allure.title("All cluster nodes are healthy and reachable")
    def test_all_nodes_healthy(self, helios_client):
        """
        Verify: every node in the cluster reports healthy status.
        Failure here means the cluster needs admin attention before running further tests.
        """
        with allure.step("Fetch cluster info from Helios API"):
            info = helios_client.get_cluster_info()

        with allure.step(f"Verify {info.node_count} nodes all healthy"):
            unhealthy = [n for n in info.nodes if not n.is_healthy]
            assert not unhealthy, (
                f"{len(unhealthy)}/{info.node_count} nodes unhealthy: "
                + ", ".join(f"{n.id}({n.status})" for n in unhealthy)
            )

        allure.attach(
            f"Total nodes: {info.node_count}\nHealthy: {info.healthy_node_count}\n"
            f"Cluster name: {info.name}",
            name="cluster_summary.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

    @allure.title("Cluster has quorum (majority of nodes elected a leader)")
    def test_quorum_ok(self, helios_client):
        """
        Verify quorum is established. Without quorum, no writes are possible
        and protection jobs will not run.
        """
        with allure.step("Check quorum status"):
            info = helios_client.get_cluster_info()

        assert info.quorum_ok, (
            f"Cluster quorum NOT OK. "
            f"Healthy nodes: {info.healthy_node_count}/{info.node_count}. "
            "Quorum requires majority."
        )

    @allure.title("A leader node is elected")
    def test_leader_elected(self, helios_client):
        """Verify exactly one node reports the leader role."""
        with allure.step("Find leader node"):
            info = helios_client.get_cluster_info()
            leaders = [n for n in info.nodes if n.is_leader]

        assert len(leaders) == 1, (
            f"Expected exactly 1 leader, found {len(leaders)}: "
            + ", ".join(n.id for n in leaders)
        )

    @allure.title("Cluster software version is consistent across all nodes")
    def test_consistent_version(self, helios_client):
        """All nodes must run the same software version. Mixed versions indicate a failed NDU."""
        with allure.step("Collect node software versions"):
            info = helios_client.get_cluster_info()
            versions = {n.id: getattr(n, "software_version", "unknown") for n in info.nodes}

        unique_versions = set(versions.values()) - {"unknown"}
        assert len(unique_versions) <= 1, (
            f"Version mismatch across nodes — possible failed NDU: {versions}"
        )
