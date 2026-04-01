"""
Root conftest.py — session-scoped fixtures shared across ALL test suites.

Fixture hierarchy:
  conftest.py (this file) — session scope: helios_client, ssh_nodes, cluster health
  tests/conftest.py       — suite scope:   protocol_client (parametrized x4)
  tests/performance/conftest.py — perf scope: baseline_store
  tests/chaos/conftest.py       — chaos scope: fault_injector, health guard

CLI options:
  --cluster-ip    Cohesity cluster management IP (required)
  --cluster-id    Cluster ID for Helios API calls (required for cluster-specific ops)
  --env           lab | staging | prod-ro (default: lab)
  --update-baselines  Re-record performance baselines (don't fail on regression)
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Generator

import pytest

from helix.api.client import HeliosClient
from helix.api.auth import APIKeyAuth
from helix.collect.artifacts import ArtifactCollector, pytest_runtest_makereport_hook
from helix.collect.stats import StatsCollector
from helix.ssh.remote import SSHClient

logger = logging.getLogger(__name__)


# ─── CLI options ─────────────────────────────────────────────────────────────

def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--cluster-ip",
        action="store",
        required=False,
        default=os.environ.get("CLUSTER_IP", ""),
        help="Cohesity cluster management IP address",
    )
    parser.addoption(
        "--cluster-id",
        action="store",
        required=False,
        default=os.environ.get("CLUSTER_ID", ""),
        help="Cohesity cluster ID (for accessClusterId header)",
    )
    parser.addoption(
        "--env",
        action="store",
        default=os.environ.get("HELIX_ENV", "lab"),
        choices=["lab", "staging", "prod-ro"],
        help="Target environment (lab allows destructive tests)",
    )
    parser.addoption(
        "--update-baselines",
        action="store_true",
        default=False,
        help="Re-record performance baselines after intentional changes",
    )
    parser.addoption(
        "--ssh-user",
        action="store",
        default=os.environ.get("SSH_USER", "cohesity"),
        help="SSH username for cluster node access",
    )
    parser.addoption(
        "--ssh-key",
        action="store",
        default=os.environ.get("SSH_KEY_PATH", "~/.ssh/id_rsa"),
        help="Path to SSH private key",
    )


# ─── Config object ────────────────────────────────────────────────────────────

class HelixConfig:
    """Typed config built from CLI options + environment variables."""

    def __init__(self, request: pytest.FixtureRequest) -> None:
        self.cluster_ip: str = request.config.getoption("--cluster-ip")
        self.cluster_id: str = request.config.getoption("--cluster-id")
        self.env: str = request.config.getoption("--env")
        self.update_baselines: bool = request.config.getoption("--update-baselines")
        self.ssh_user: str = request.config.getoption("--ssh-user")
        self.ssh_key_path: str = request.config.getoption("--ssh-key")
        self.api_key: str = os.environ.get("HELIOS_API_KEY", "")

        # Protocol-specific config from env vars
        self.nfs_export: str = os.environ.get("NFS_EXPORT", f"/cohesity-test")
        self.smb_share: str = os.environ.get("SMB_SHARE", "test-share")
        self.smb_user: str = os.environ.get("SMB_USER", "testuser")
        self.smb_password: str = os.environ.get("SMB_PASSWORD", "")
        self.s3_bucket: str = os.environ.get("S3_BUCKET", "helix-test-bucket")
        self.s3_access_key: str = os.environ.get("S3_ACCESS_KEY", "")
        self.s3_secret_key: str = os.environ.get("S3_SECRET_KEY", "")
        self.iscsi_target: str = os.environ.get("ISCSI_TARGET", "")
        self.iscsi_portal: str = os.environ.get("ISCSI_PORTAL", self.cluster_ip)

        # Baseline store config
        self.baseline_backend: str = os.environ.get("BASELINE_BACKEND", "json")
        self.baseline_dir: str = os.environ.get("BASELINE_DIR", "baselines")

    @property
    def node_ips(self) -> list[str]:
        """Parse CLUSTER_NODE_IPS env var (comma-separated)."""
        raw = os.environ.get("CLUSTER_NODE_IPS", "")
        return [ip.strip() for ip in raw.split(",") if ip.strip()]

    def require_cluster_ip(self) -> None:
        if not self.cluster_ip:
            pytest.skip("--cluster-ip not provided; skipping cluster tests")

    def require_lab_env(self) -> None:
        if self.env != "lab":
            pytest.skip(f"Destructive test requires --env=lab (current: {self.env})")


@pytest.fixture(scope="session")
def helix_config(request: pytest.FixtureRequest) -> HelixConfig:
    return HelixConfig(request)


# ─── Session-scoped fixtures ──────────────────────────────────────────────────

@pytest.fixture(scope="session")
def helios_client(helix_config: HelixConfig) -> Generator[HeliosClient, None, None]:
    """
    Authenticated Helios API client for the entire test session.
    Skips if no API key configured (unit-test mode).
    """
    if not helix_config.api_key:
        pytest.skip("HELIOS_API_KEY not set; skipping Helios API tests")

    auth = APIKeyAuth(helix_config.api_key)
    client = HeliosClient(
        auth=auth,
        cluster_id=helix_config.cluster_id or None,
    )

    logger.info("helios_client: connecting to Helios API")
    yield client
    client.close()
    logger.info("helios_client: session closed")


@pytest.fixture(scope="session")
def ssh_nodes(helix_config: HelixConfig) -> Generator[dict[str, SSHClient], None, None]:
    """
    Open SSH connections to all cluster nodes.
    Returns {node_id: SSHClient} — e.g., {"node-1": SSHClient(...), "node-2": ...}
    Skips if no node IPs configured.
    """
    node_ips = helix_config.node_ips
    if not node_ips:
        pytest.skip("CLUSTER_NODE_IPS not set; skipping SSH-dependent tests")

    nodes: dict[str, SSHClient] = {}
    for i, ip in enumerate(node_ips, start=1):
        node_id = f"node-{i}"
        ssh = SSHClient(
            hostname=ip,
            username=helix_config.ssh_user,
            key_filename=os.path.expanduser(helix_config.ssh_key_path),
        )
        ssh.connect()
        nodes[node_id] = ssh
        logger.info("ssh_nodes: connected to %s (%s)", node_id, ip)

    yield nodes

    for node_id, ssh in nodes.items():
        try:
            ssh.disconnect()
            logger.info("ssh_nodes: disconnected from %s", node_id)
        except Exception as e:
            logger.warning("ssh_nodes: error disconnecting %s: %s", node_id, e)


@pytest.fixture(scope="session", autouse=True)
def verify_cluster_health(helios_client: HeliosClient) -> Generator[None, None, None]:
    """
    Autouse: verify cluster quorum before running ANY tests.
    Skips entire suite if cluster is unhealthy.
    Post-suite check warns if tests left cluster degraded.
    """
    info = helios_client.get_cluster_info()
    if not info.quorum_ok:
        pytest.skip(
            f"Cluster quorum not OK — {info.healthy_node_count}/{info.node_count} nodes healthy. "
            "Fix cluster before running tests."
        )
    logger.info(
        "verify_cluster_health: cluster OK (%d/%d nodes healthy)",
        info.healthy_node_count, info.node_count,
    )

    yield

    # Post-suite health check
    try:
        post_info = helios_client.get_cluster_info()
        if not post_info.quorum_ok:
            logger.error(
                "verify_cluster_health: CLUSTER DEGRADED after test suite! "
                "(%d/%d nodes healthy) — check for unfixed faults",
                post_info.healthy_node_count, post_info.node_count,
            )
        else:
            logger.info("verify_cluster_health: post-suite health OK")
    except Exception as e:
        logger.warning("verify_cluster_health: post-suite check failed: %s", e)


@pytest.fixture(scope="session", autouse=True)
def stats_collector(ssh_nodes: dict[str, SSHClient]) -> Generator[StatsCollector, None, None]:
    """
    Autouse: continuous background stats sampling for all nodes throughout the session.
    Writes host_stats.csv to allure-results/ at teardown.
    """
    collector = StatsCollector(ssh_nodes, interval_secs=5)
    collector.start()

    yield collector

    collector.stop()
    csv_path = "allure-results/host_stats.csv"
    collector.write_csv(csv_path)
    collector.attach_to_allure(csv_path)


@pytest.fixture
def artifact_collector(
    ssh_nodes: dict[str, SSHClient],
    helios_client: HeliosClient,
    helix_config: HelixConfig,
) -> Generator[ArtifactCollector, None, None]:
    """
    Per-test artifact collector. Used by pytest_runtest_makereport hook
    to pull logs automatically on failure.
    """
    collector = ArtifactCollector(
        ssh_nodes=ssh_nodes,
        helios_client=helios_client,
        cluster_id=helix_config.cluster_id or None,
    )
    yield collector


# ─── Failure hook ─────────────────────────────────────────────────────────────

@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo) -> None:
    """Collect cluster artifacts automatically when a test fails."""
    outcome = yield
    pytest_runtest_makereport_hook(item, call, outcome)
