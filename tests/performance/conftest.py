"""
tests/performance/conftest.py — performance test fixtures.

Provides baseline_store and baseline_comparator to all performance tests.
"""

from __future__ import annotations

import pytest

from helix.baseline.store import BaselineStore
from helix.baseline.comparator import BaselineComparator
from helix.baseline.reporter import BaselineReporter


@pytest.fixture(scope="session")
def baseline_store(helix_config, request: pytest.FixtureRequest) -> BaselineStore:
    """
    Session-scoped baseline store.
    --update-baselines flag puts store in update mode (records new values but
    still reports comparisons without failing on regressions).
    """
    store = BaselineStore.from_config(helix_config)
    store.set_update_mode(request.config.getoption("--update-baselines"))
    return store


@pytest.fixture(scope="session")
def baseline_comparator(baseline_store: BaselineStore) -> BaselineComparator:
    """Configured comparator: 5% hard regression threshold, 2σ statistical detection."""
    return BaselineComparator(
        store=baseline_store,
        hard_regression_pct=5.0,
        warning_pct=2.0,
        stddev_multiplier=2.0,
        min_samples=3,
    )


@pytest.fixture(scope="session")
def baseline_reporter() -> BaselineReporter:
    return BaselineReporter()


@pytest.fixture
def fio_runner(helix_config, ssh_nodes):
    """FioRunner targeting the cluster's NFS export (runs fio on local mount)."""
    from helix.tools.fio import FioRunner
    # Run fio locally against the mounted NFS path
    return FioRunner(ssh_client=None)


@pytest.fixture
def vdbench_runner(helix_config, ssh_nodes):
    """VdbenchRunner targeting the first available cluster node via SSH."""
    from helix.tools.vdbench import VdbenchRunner
    if not ssh_nodes:
        pytest.skip("No SSH nodes available for vdbench")
    first_node = next(iter(ssh_nodes.values()))
    return VdbenchRunner(ssh_client=first_node)
