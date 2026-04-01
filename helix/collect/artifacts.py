"""
ArtifactCollector — pulls logs and diagnostic data from Cohesity cluster on test failure.

Triggered automatically by pytest_runtest_makereport hook when a test fails.
Collects from two sources:
  1. Helios management API — cluster events, alerts, job run history, node diagnostics
  2. SSH to cluster nodes — iris logs, bridge logs, systemd journal, kernel messages

All artifacts are attached to the current Allure test as separate attachments,
making failure analysis self-contained in the test report.

Usage (in conftest.py):
    @pytest.fixture(autouse=True)
    def artifact_collector(ssh_nodes, helios_client, request):
        collector = ArtifactCollector(ssh_nodes, helios_client)
        yield collector
        # Collection happens via pytest_runtest_makereport hook
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from helix.ssh.remote import SSHClient
    from helix.api.client import HeliosClient

logger = logging.getLogger(__name__)


@dataclass
class CollectedArtifact:
    name: str
    content: str
    attachment_type: str = "TEXT"   # "TEXT" | "JSON"


class ArtifactCollector:
    """
    On-demand artifact collection triggered by test failures.

    Pulls from Helios API (management plane) and SSH (host logs)
    in parallel threads to minimize collection time.
    """

    # Lines to pull from each log file on failure
    LOG_TAIL_LINES = 500

    # Helios API paths for diagnostics
    HELIOS_EVENTS_PATH = "v2/mcm/clusters/{cluster_id}/events"
    HELIOS_ALERTS_PATH = "v2/mcm/alerts"

    def __init__(
        self,
        ssh_nodes: dict[str, "SSHClient"],
        helios_client: "HeliosClient | None" = None,
        cluster_id: str | None = None,
    ) -> None:
        self._ssh_nodes = ssh_nodes
        self._helios = helios_client
        self._cluster_id = cluster_id
        self._artifacts: list[CollectedArtifact] = []
        self._lock = threading.Lock()

    def collect_all(self) -> list[CollectedArtifact]:
        """
        Collect all artifacts in parallel (API + SSH).
        Returns list of CollectedArtifact for programmatic inspection.
        """
        self._artifacts.clear()
        threads = []

        if self._helios:
            t = threading.Thread(target=self._collect_helios, daemon=True)
            threads.append(t)

        for node_id, ssh in self._ssh_nodes.items():
            t = threading.Thread(target=self._collect_node, args=(node_id, ssh), daemon=True)
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        self._attach_to_allure()
        logger.info("ArtifactCollector: collected %d artifacts", len(self._artifacts))
        return list(self._artifacts)

    # ─── Helios API collection ────────────────────────────────────────────────

    def _collect_helios(self) -> None:
        """Collect cluster events, alerts, and job history from Helios API."""
        try:
            self._collect_helios_events()
            self._collect_helios_alerts()
        except Exception as e:
            logger.error("ArtifactCollector: Helios collection failed: %s", e)

    def _collect_helios_events(self) -> None:
        try:
            resp = self._helios._session.get(
                f"{self._helios.base_url}v2/mcm/audit/logs",
                params={"maxLogs": 100},
                timeout=15,
            )
            if resp.ok:
                self._add_artifact("helios_events.json", resp.text, "JSON")
        except Exception as e:
            logger.warning("ArtifactCollector: events collection failed: %s", e)

    def _collect_helios_alerts(self) -> None:
        try:
            resp = self._helios._session.get(
                f"{self._helios.base_url}v2/mcm/alerts",
                params={"maxAlerts": 50},
                timeout=15,
            )
            if resp.ok:
                self._add_artifact("helios_alerts.json", resp.text, "JSON")
        except Exception as e:
            logger.warning("ArtifactCollector: alerts collection failed: %s", e)

    # ─── SSH log collection ───────────────────────────────────────────────────

    def _collect_node(self, node_id: str, ssh: "SSHClient") -> None:
        """Pull log files and system state from a cluster node via SSH."""
        prefix = f"node_{node_id}"
        commands = [
            # Cohesity service logs
            (f"{prefix}_iris.log", f"sudo tail -{self.LOG_TAIL_LINES} /cohesity/logs/iris/iris.INFO 2>/dev/null || echo 'file not found'"),
            (f"{prefix}_bridge.log", f"sudo tail -{self.LOG_TAIL_LINES} /cohesity/logs/bridge/bridge.INFO 2>/dev/null || echo 'file not found'"),
            # systemd journal
            (f"{prefix}_journal.log", "sudo journalctl -u cohesity --since '10 minutes ago' --no-pager 2>/dev/null"),
            # Kernel messages (disk errors, OOM events)
            (f"{prefix}_dmesg.log", "sudo dmesg | tail -100 2>/dev/null"),
            # Disk layout
            (f"{prefix}_disk_layout.txt", "df -h && echo '---' && lsblk -o NAME,SIZE,TYPE,MOUNTPOINT 2>/dev/null"),
            # Network state
            (f"{prefix}_netstat.txt", "ss -tlnp 2>/dev/null || netstat -tlnp 2>/dev/null"),
            # Process list (Cohesity processes)
            (f"{prefix}_processes.txt", "ps aux | grep -E '(cohesity|iris|bridge|magneto)' | grep -v grep 2>/dev/null"),
        ]

        for artifact_name, cmd in commands:
            try:
                result = ssh.run(cmd, check=False)
                if result.stdout.strip():
                    self._add_artifact(artifact_name, result.stdout, "TEXT")
                elif result.stderr.strip():
                    logger.debug("ArtifactCollector: %s had no stdout; stderr: %s", artifact_name, result.stderr[:200])
            except Exception as e:
                logger.warning("ArtifactCollector: failed to collect %s from %s: %s", artifact_name, node_id, e)

    # ─── Allure attachment ────────────────────────────────────────────────────

    def _attach_to_allure(self) -> None:
        """Attach all collected artifacts to the current Allure test."""
        try:
            import allure
        except ImportError:
            return

        for artifact in self._artifacts:
            attachment_type = (
                allure.attachment_type.JSON
                if artifact.attachment_type == "JSON"
                else allure.attachment_type.TEXT
            )
            allure.attach(
                artifact.content,
                name=artifact.name,
                attachment_type=attachment_type,
            )

    # ─── Internal helpers ─────────────────────────────────────────────────────

    def _add_artifact(self, name: str, content: str, attachment_type: str = "TEXT") -> None:
        with self._lock:
            self._artifacts.append(CollectedArtifact(
                name=name, content=content, attachment_type=attachment_type
            ))


# ─── pytest hook integration ──────────────────────────────────────────────────

def pytest_runtest_makereport_hook(item, call, outcome):
    """
    Pytest hook that triggers artifact collection on test failure.

    Register in conftest.py:
        @pytest.hookimpl(tryfirst=True, hookwrapper=True)
        def pytest_runtest_makereport(item, call):
            outcome = yield
            pytest_runtest_makereport_hook(item, call, outcome)
    """
    rep = outcome.get_result()
    if rep.failed and call.when == "call":
        collector = item.funcargs.get("artifact_collector")
        if collector and isinstance(collector, ArtifactCollector):
            logger.info("ArtifactCollector: test failed, collecting artifacts for %s", item.nodeid)
            collector.collect_all()
