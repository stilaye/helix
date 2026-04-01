"""
tests/chaos/conftest.py — chaos test fixtures.

Key safety mechanisms:
  1. ensure_healthy_before_chaos (autouse): skips chaos test if cluster is already degraded.
     Prevents "chaos test on a broken cluster" which produces misleading failures.
  2. fault_injector fixture: always calls heal_all() in teardown, even on assertion failure.
     Prevents iptables rules / killed services persisting into the next test.
"""

from __future__ import annotations

import logging

import pytest

from helix.fault.injector import FaultInjector

logger = logging.getLogger(__name__)


@pytest.fixture(autouse=True)
def ensure_healthy_before_chaos(helios_client) -> None:
    """
    Guard: skip chaos test if cluster is not healthy.

    Running chaos on an already-degraded cluster produces cascading failures
    that hide the real root cause and leave the cluster in an even worse state.
    """
    info = helios_client.get_cluster_info()
    if not info.quorum_ok:
        pytest.skip(
            f"Cluster not healthy before chaos test "
            f"({info.healthy_node_count}/{info.node_count} nodes up). "
            "Fix cluster before running chaos tests."
        )
    logger.info(
        "ensure_healthy_before_chaos: cluster OK (%d/%d nodes), proceeding",
        info.healthy_node_count, info.node_count,
    )
    yield
    # No post-test cleanup here — fault_injector.heal_all() handles that


@pytest.fixture
def fault_injector(ssh_nodes, helios_client) -> FaultInjector:
    """
    FaultInjector with guaranteed cleanup via heal_all() in teardown.

    heal_all() removes:
    - iptables DROP rules (network partition)
    - tc netem disciplines (latency/loss/corruption)
    - restarts killed/stopped services

    This runs EVEN if the test assertion fails or raises an exception.
    Prevents "chaos test left cluster broken for 50 tests" — a real CI failure mode.
    """
    injector = FaultInjector(
        ssh_nodes=ssh_nodes,
        helios_client=helios_client,
    )
    yield injector
    logger.info("fault_injector teardown: running heal_all()")
    injector.heal_all()


@pytest.fixture
def require_lab_env(helix_config) -> None:
    """
    Guard for destructive tests marked @pytest.mark.destructive.
    Only run in --env=lab to protect staging/prod clusters.
    """
    helix_config.require_lab_env()
