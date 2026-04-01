"""
Network fault injection via iptables and tc netem.

Context manager guarantees cleanup even if test assertion throws.
This prevents the "chaos test left cluster broken" problem that causes
cascading failures in CI — the most important safety property.

Usage:
    with NetworkFault(ssh=ssh_nodes["node-1"]) as fault:
        fault.partition_from(["10.0.0.2", "10.0.0.3"])
        # test code here
    # iptables rules automatically removed on __exit__

Or for one-shot injection:
    fault = NetworkFault(ssh=ssh_nodes["node-1"])
    fault.add_latency(delay_ms=100, jitter_ms=20)
    try:
        # run test
    finally:
        fault.heal()
"""

from __future__ import annotations

import logging
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from helix.ssh.remote import SSHClient

logger = logging.getLogger(__name__)


class NetworkFault:
    """
    Inject network faults via iptables (partition) and tc netem (latency/loss/corruption).

    Args:
        ssh: SSHClient connected to the node where faults will be injected.
        interface: Network interface to attach tc disciplines to (default: eth0).
    """

    def __init__(self, ssh: "SSHClient", interface: str = "eth0") -> None:
        self._ssh = ssh
        self._iface = interface
        self._iptables_rules: list[str] = []    # Track for cleanup
        self._tc_active: bool = False

    # ─── Partition (iptables) ─────────────────────────────────────────────────

    def partition_from(self, target_ips: list[str]) -> None:
        """
        Block all traffic to/from target IPs (network partition simulation).
        Uses iptables DROP rules — hard partition, no RST sent.
        """
        for ip in target_ips:
            # Block outbound
            rule_out = f"-A OUTPUT -d {ip} -j DROP"
            self._ssh.run(f"sudo iptables {rule_out}", check=True)
            self._iptables_rules.append(rule_out)
            # Block inbound
            rule_in = f"-A INPUT -s {ip} -j DROP"
            self._ssh.run(f"sudo iptables {rule_in}", check=True)
            self._iptables_rules.append(rule_in)
            logger.info("NetworkFault: partitioned from %s", ip)

    def unpartition_from(self, target_ips: list[str]) -> None:
        """Re-allow traffic to/from specific IPs."""
        for ip in target_ips:
            self._ssh.run(f"sudo iptables -D OUTPUT -d {ip} -j DROP", check=False)
            self._ssh.run(f"sudo iptables -D INPUT -s {ip} -j DROP", check=False)
        # Remove from tracking
        self._iptables_rules = [
            r for r in self._iptables_rules
            if not any(ip in r for ip in target_ips)
        ]

    # ─── Latency / Loss / Corruption (tc netem) ──────────────────────────────

    def add_latency(self, delay_ms: int, jitter_ms: int = 10) -> None:
        """Add artificial network latency using tc netem."""
        self._tc_setup()
        self._ssh.run(
            f"sudo tc qdisc change dev {self._iface} root netem "
            f"delay {delay_ms}ms {jitter_ms}ms distribution normal",
            check=True,
        )
        self._tc_active = True
        logger.info("NetworkFault: %dms latency (+%dms jitter) on %s", delay_ms, jitter_ms, self._iface)

    def add_packet_loss(self, loss_pct: float) -> None:
        """Inject random packet loss (e.g., loss_pct=5.0 for 5% packet loss)."""
        self._tc_setup()
        self._ssh.run(
            f"sudo tc qdisc change dev {self._iface} root netem loss {loss_pct:.1f}%",
            check=True,
        )
        self._tc_active = True
        logger.info("NetworkFault: %.1f%% packet loss on %s", loss_pct, self._iface)

    def corrupt_packets(self, corruption_pct: float) -> None:
        """Corrupt a percentage of packets (simulates bit errors)."""
        self._tc_setup()
        self._ssh.run(
            f"sudo tc qdisc change dev {self._iface} root netem corrupt {corruption_pct:.1f}%",
            check=True,
        )
        self._tc_active = True
        logger.info("NetworkFault: %.1f%% packet corruption on %s", corruption_pct, self._iface)

    def _tc_setup(self) -> None:
        """Initialize tc qdisc if not already set up."""
        if not self._tc_active:
            self._ssh.run(
                f"sudo tc qdisc add dev {self._iface} root netem delay 0ms",
                check=False,  # May already exist
            )

    # ─── Cleanup ─────────────────────────────────────────────────────────────

    def heal(self) -> None:
        """
        Remove ALL active iptables rules and tc disciplines injected by this instance.
        Called automatically by __exit__ — guaranteed to run even on test failure.
        """
        # Remove iptables rules in reverse order
        for rule in reversed(self._iptables_rules):
            delete_rule = rule.replace("-A ", "-D ", 1)
            self._ssh.run(f"sudo iptables {delete_rule}", check=False)
        self._iptables_rules.clear()

        # Remove tc netem
        if self._tc_active:
            self._ssh.run(
                f"sudo tc qdisc del dev {self._iface} root netem",
                check=False,
            )
            self._tc_active = False

        logger.info("NetworkFault: healed — all rules removed on %s", self._iface)

    # ─── Context manager ─────────────────────────────────────────────────────

    def __enter__(self) -> "NetworkFault":
        return self

    def __exit__(self, *_: Any) -> None:
        self.heal()  # ALWAYS runs, even on assertion failure

    def __repr__(self) -> str:
        return f"NetworkFault(iface={self._iface}, rules={len(self._iptables_rules)}, tc={self._tc_active})"
