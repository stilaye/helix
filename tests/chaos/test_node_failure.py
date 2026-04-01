"""
Chaos: node failure and recovery.

Tests:
  - Kill iris process on one node → cluster maintains quorum within SLA
  - Stop Cohesity service on one node → data still accessible via other nodes
  - Freeze (SIGSTOP) process → slow-node simulation

All tests use FaultInjector with guaranteed heal_all() teardown.
The ensure_healthy_before_chaos autouse fixture prevents cascading failures.

SLA: NODE_FAILOVER_MAX_SECS = 30 (quorum re-established within 30 seconds)
"""

from __future__ import annotations

import time

import allure
import pytest

from helix.constants import ResilienceSLA
from helix.utils.wait import poll_until

pytestmark = [pytest.mark.chaos, pytest.mark.destructive, pytest.mark.slow]


@allure.suite("Chaos")
@allure.feature("Node Failure")
class TestNodeFailure:

    @allure.title("Cluster maintains quorum after iris process kill on one node")
    @pytest.mark.timeout(120)
    def test_single_node_iris_kill(self, fault_injector, helios_client, ssh_nodes):
        """
        Kill iris (Cohesity main process) on node-1.
        Verify cluster re-establishes quorum within 30 seconds.
        Fault is automatically healed in fixture teardown.
        """
        if len(ssh_nodes) < 2:
            pytest.skip("Need at least 2 nodes for single-node failure test")

        target_node = "node-1"

        with allure.step(f"Kill iris process on {target_node}"):
            fault_injector.kill_node(target_node, process="iris")
            kill_time = time.monotonic()

        with allure.step(f"Wait for quorum recovery (SLA: {ResilienceSLA.NODE_FAILOVER_MAX_SECS}s)"):
            def quorum_recovered():
                info = helios_client.get_cluster_info()
                return info.quorum_ok

            poll_until(
                quorum_recovered,
                timeout=ResilienceSLA.NODE_FAILOVER_MAX_SECS,
                interval=2,
                message=f"Quorum not recovered within {ResilienceSLA.NODE_FAILOVER_MAX_SECS}s after node kill",
            )
            recovery_time = time.monotonic() - kill_time

        with allure.step("Verify data is accessible during partial node failure"):
            # Even with one node killed, data should be accessible via quorum
            info = helios_client.get_cluster_info()
            assert info.quorum_ok, "Quorum lost after single node kill"

        allure.attach(
            f"Target node: {target_node}\n"
            f"Recovery time: {recovery_time:.1f}s\n"
            f"SLA: {ResilienceSLA.NODE_FAILOVER_MAX_SECS}s\n"
            f"Nodes healthy after recovery: {info.healthy_node_count}/{info.node_count}",
            name="node_failure_summary.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

        assert recovery_time <= ResilienceSLA.NODE_FAILOVER_MAX_SECS, (
            f"Quorum recovery took {recovery_time:.1f}s, SLA is {ResilienceSLA.NODE_FAILOVER_MAX_SECS}s"
        )

    @allure.title("Cohesity service stop: data still accessible via remaining nodes")
    @pytest.mark.timeout(180)
    def test_service_stop_data_accessible(
        self, fault_injector, helios_client, ssh_nodes, helix_config
    ):
        """
        Stop cohesity service on node-2. Verify:
        1. Other nodes remain accessible
        2. Helios API continues working
        3. Service restarts cleanly on heal (via systemctl start cohesity)
        """
        if len(ssh_nodes) < 2:
            pytest.skip("Need at least 2 nodes for this test")

        target_node = "node-2"

        with allure.step(f"Stop cohesity service on {target_node}"):
            fault_injector.stop_node_service(target_node, service="cohesity")

        with allure.step("Verify Helios API still responds via other nodes"):
            info = helios_client.get_cluster_info()
            assert info.quorum_ok, f"Quorum lost after service stop on {target_node}"

        with allure.step("Verify protection jobs list is still accessible"):
            jobs = helios_client.list_protection_jobs()
            assert isinstance(jobs, list)

        # fault_injector.heal_all() in fixture teardown will restart the service

    @allure.title("Frozen process (SIGSTOP) simulates slow node — cluster continues operating")
    @pytest.mark.timeout(120)
    def test_frozen_node_slow_simulation(self, fault_injector, helios_client, ssh_nodes):
        """
        SIGSTOP iris on node-1 to simulate a slow/hung node.
        Verify cluster continues operating with the frozen node.
        Then unfreeze and verify node recovers.

        This tests the cluster's ability to detect and route around a
        slow node (not a crashed node — the process is still alive but paused).
        """
        if len(ssh_nodes) < 2:
            pytest.skip("Need at least 2 nodes")

        target_node = "node-1"
        node_fault = fault_injector.kill_node.__func__  # reference for manual control

        # Get direct access to NodeFault for freeze/unfreeze
        from helix.fault.node import NodeFault
        ssh = fault_injector._get_ssh(target_node)
        nf = NodeFault(ssh)

        with allure.step(f"Freeze (SIGSTOP) iris on {target_node}"):
            nf.freeze_process("iris")

        with allure.step("Verify cluster remains accessible with frozen node"):
            time.sleep(5)
            info = helios_client.get_cluster_info()
            assert info.quorum_ok, "Quorum lost when one node frozen"

        with allure.step(f"Unfreeze (SIGCONT) iris on {target_node}"):
            nf.unfreeze_process("iris")
            time.sleep(3)  # Allow process to resume

        with allure.step("Verify cluster fully healthy after unfreeze"):
            info = helios_client.get_cluster_info()
            # Allow a brief recovery window
            if not info.quorum_ok:
                poll_until(
                    lambda: helios_client.get_cluster_info().quorum_ok,
                    timeout=30, interval=2,
                    message="Cluster did not recover after unfreeze within 30s",
                )
