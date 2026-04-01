"""
FaultInjector facade — coordinates network, node, and disk faults across multiple nodes.

Used as the primary fixture in chaos tests:
    @pytest.fixture
    def fault_injector(ssh_nodes, helios_client) -> FaultInjector:
        injector = FaultInjector(ssh_nodes, helios_client)
        yield injector
        injector.heal_all()   # guaranteed cleanup
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from helix.fault.network import NetworkFault
from helix.fault.node import NodeFault, DiskFault

if TYPE_CHECKING:
    from helix.ssh.remote import SSHClient
    from helix.api.client import HeliosClient

logger = logging.getLogger(__name__)


class FaultInjector:
    """
    Facade for all fault injection operations across multiple cluster nodes.

    Maintains a registry of active faults and provides heal_all() for
    guaranteed cleanup in pytest fixture teardown.
    """

    def __init__(
        self,
        ssh_nodes: dict[str, "SSHClient"],
        helios_client: "HeliosClient | None" = None,
        default_interface: str = "eth0",
    ) -> None:
        self._ssh_nodes = ssh_nodes
        self._helios = helios_client
        self._default_iface = default_interface
        self._active_network_faults: list[NetworkFault] = []
        self._active_node_faults: list[NodeFault] = []
        self._active_disk_faults: list[DiskFault] = []

    # ─── Node faults ──────────────────────────────────────────────────────────

    def kill_node(self, node_id: str, process: str = "iris") -> NodeFault:
        """Kill a process on the specified node. Returns the NodeFault for manual control."""
        ssh = self._get_ssh(node_id)
        fault = NodeFault(ssh)
        fault.kill_process(process)
        self._active_node_faults.append(fault)
        return fault

    def stop_node_service(self, node_id: str, service: str = "cohesity") -> NodeFault:
        """Gracefully stop a service on the specified node."""
        ssh = self._get_ssh(node_id)
        fault = NodeFault(ssh)
        fault.stop_service(service)
        self._active_node_faults.append(fault)
        return fault

    # ─── Network faults ───────────────────────────────────────────────────────

    def partition_nodes(
        self, node_id: str, from_ips: list[str], interface: str | None = None
    ) -> NetworkFault:
        """Block traffic between node_id and a list of IP addresses."""
        ssh = self._get_ssh(node_id)
        fault = NetworkFault(ssh, interface=interface or self._default_iface)
        fault.partition_from(from_ips)
        self._active_network_faults.append(fault)
        return fault

    def add_latency(
        self, node_id: str, delay_ms: int, jitter_ms: int = 10, interface: str | None = None
    ) -> NetworkFault:
        """Inject latency on a node's network interface."""
        ssh = self._get_ssh(node_id)
        fault = NetworkFault(ssh, interface=interface or self._default_iface)
        fault.add_latency(delay_ms=delay_ms, jitter_ms=jitter_ms)
        self._active_network_faults.append(fault)
        return fault

    def add_packet_loss(
        self, node_id: str, loss_pct: float, interface: str | None = None
    ) -> NetworkFault:
        """Inject packet loss on a node's network interface."""
        ssh = self._get_ssh(node_id)
        fault = NetworkFault(ssh, interface=interface or self._default_iface)
        fault.add_packet_loss(loss_pct)
        self._active_network_faults.append(fault)
        return fault

    # ─── Disk faults ──────────────────────────────────────────────────────────

    def inject_disk_errors(self, node_id: str, device: str) -> DiskFault:
        """Inject disk I/O errors on a specific device."""
        ssh = self._get_ssh(node_id)
        fault = DiskFault(ssh)
        fault.inject_errors(device)
        self._active_disk_faults.append(fault)
        return fault

    # ─── Cleanup ─────────────────────────────────────────────────────────────

    def heal_all(self) -> None:
        """
        Remove all active faults: iptables rules, tc disciplines, killed services.
        Called in pytest fixture teardown — guaranteed to run even on test failure.
        """
        for fault in self._active_network_faults:
            try:
                fault.heal()
            except Exception as e:
                logger.error("Error healing network fault: %s", e)

        for fault in self._active_node_faults:
            try:
                fault.recover()
            except Exception as e:
                logger.error("Error recovering node fault: %s", e)

        for fault in self._active_disk_faults:
            try:
                fault.heal()
            except Exception as e:
                logger.error("Error healing disk fault: %s", e)

        self._active_network_faults.clear()
        self._active_node_faults.clear()
        self._active_disk_faults.clear()
        logger.info("FaultInjector: heal_all() complete")

    def _get_ssh(self, node_id: str) -> "SSHClient":
        ssh = self._ssh_nodes.get(node_id)
        if ssh is None:
            available = list(self._ssh_nodes.keys())
            raise KeyError(f"Node '{node_id}' not found. Available nodes: {available}")
        return ssh
