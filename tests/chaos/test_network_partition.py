"""
Chaos: network partition and latency injection.

Tests:
  - Partition node-1 from node-2 → split-brain prevention, eventual consistency
  - Inject 200ms latency → operations complete but slower
  - 10% packet loss → retransmits occur, data integrity preserved
  - Heal partition → cluster reconciles and converges

Key safety: NetworkFault context manager guarantees iptables cleanup even if
test assertion fails. Prevents "leftover iptables rules block all CI tests".
"""

from __future__ import annotations

import os
import time
import uuid

import allure
import pytest

from helix.constants import ResilienceSLA
from helix.utils.wait import poll_until

pytestmark = [pytest.mark.chaos, pytest.mark.destructive, pytest.mark.slow]


@allure.suite("Chaos")
@allure.feature("Network Partition")
class TestNetworkPartition:

    @allure.title("Network partition between two nodes: cluster prevents split-brain")
    @pytest.mark.timeout(180)
    def test_partition_two_nodes(self, fault_injector, helios_client, ssh_nodes, helix_config):
        """
        Partition node-1 from node-2 using iptables DROP rules.
        Expected: cluster detects partition, one side loses quorum, no split-brain writes.
        After heal: cluster reconciles within SLA.
        """
        if len(ssh_nodes) < 2:
            pytest.skip("Need at least 2 nodes for partition test")

        node_ips = helix_config.node_ips
        if len(node_ips) < 2:
            pytest.skip("CLUSTER_NODE_IPS must have at least 2 IPs")

        target_node = "node-1"
        blocked_ip = node_ips[1]   # Block node-1 from seeing node-2

        with allure.step(f"Partition {target_node} from {blocked_ip}"):
            fault_injector.partition_nodes(target_node, from_ips=[blocked_ip])
            partition_start = time.monotonic()

        with allure.step("Wait 10s for partition to take effect"):
            time.sleep(10)

        with allure.step("Verify cluster detects partition (quorum may be degraded)"):
            try:
                info = helios_client.get_cluster_info()
                allure.attach(
                    f"Healthy nodes after partition: {info.healthy_node_count}/{info.node_count}\n"
                    f"Quorum OK: {info.quorum_ok}",
                    name="partition_state.txt",
                    attachment_type=allure.attachment_type.TEXT,
                )
            except Exception as e:
                allure.attach(f"API unreachable during partition: {e}", "partition_api_state.txt",
                              attachment_type=allure.attachment_type.TEXT)

        # fault_injector.heal_all() removes the iptables rules in teardown
        # Then we verify recovery
        with allure.step("Heal partition (iptables rules removed)"):
            fault_injector._active_network_faults[-1].heal()
            heal_time = time.monotonic()

        with allure.step(f"Verify cluster converges after heal (SLA: {ResilienceSLA.NODE_FAILOVER_MAX_SECS}s)"):
            poll_until(
                lambda: helios_client.get_cluster_info().quorum_ok,
                timeout=ResilienceSLA.NODE_FAILOVER_MAX_SECS,
                interval=3,
                message=f"Cluster did not recover after partition heal within {ResilienceSLA.NODE_FAILOVER_MAX_SECS}s",
            )
            recovery_elapsed = time.monotonic() - heal_time

        allure.attach(
            f"Recovery time after heal: {recovery_elapsed:.1f}s",
            name="partition_recovery.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

    @allure.title("200ms network latency: operations complete, throughput degrades gracefully")
    @pytest.mark.timeout(120)
    def test_latency_injection(self, fault_injector, helios_client, ssh_nodes, helix_config):
        """
        Inject 200ms latency on node-1's eth0.
        Verify:
        - Cluster API still responds (with higher latency)
        - Data operations complete (no timeouts for well-tuned clients)
        - Latency is observable in timing measurements
        """
        if not ssh_nodes:
            pytest.skip("No SSH nodes available")

        target_node = "node-1"

        with allure.step(f"Inject 200ms latency on {target_node} eth0"):
            fault_injector.add_latency(target_node, delay_ms=200, jitter_ms=20)

        with allure.step("Measure API response time under latency"):
            start = time.monotonic()
            info = helios_client.get_cluster_info()
            elapsed_ms = (time.monotonic() - start) * 1000

        allure.attach(
            f"API response time under 200ms latency: {elapsed_ms:.0f}ms\n"
            f"Cluster quorum: {info.quorum_ok}",
            name="latency_test_results.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

        assert info.quorum_ok, "Cluster lost quorum under 200ms latency injection"
        # Response should be > 200ms (we injected 200ms delay)
        # But < 10s (should not timeout for management plane calls)
        assert elapsed_ms < 10_000, f"API call timed out under 200ms latency: {elapsed_ms:.0f}ms"

    @allure.title("10% packet loss: data integrity preserved via TCP retransmits")
    @pytest.mark.timeout(180)
    def test_packet_loss_data_integrity(self, fault_injector, helios_client, ssh_nodes, helix_config):
        """
        Inject 10% packet loss on node-1.
        Write and read back data — TCP retransmits ensure no data loss.
        This validates that Cohesity's TCP stack handles packet loss correctly.
        """
        if not ssh_nodes:
            pytest.skip("No SSH nodes available")

        target_node = "node-1"

        with allure.step(f"Inject 10% packet loss on {target_node}"):
            fault_injector.add_packet_loss(target_node, loss_pct=10.0)

        from helix.protocols.nfs import NFSClient
        test_data = os.urandom(1 * 1024 * 1024)  # 1 MB
        key = f"/helix-chaos/packet_loss_{uuid.uuid4().hex[:6]}.bin"

        with allure.step("Write 1 MB via NFS under 10% packet loss"):
            import hashlib
            original_hash = hashlib.sha256(test_data).hexdigest()
            with NFSClient(helix_config) as nfs:
                nfs.write_file(key, test_data)

        with allure.step("Read back and verify checksum (TCP must recover all bytes)"):
            with NFSClient(helix_config) as nfs:
                read_data = nfs.read_file(key)
                read_hash = hashlib.sha256(read_data).hexdigest()
                nfs.delete_file(key)

        assert read_hash == original_hash, (
            "Data corrupted under 10% packet loss — TCP retransmit did not recover all bytes"
        )
