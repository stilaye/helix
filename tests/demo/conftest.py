"""
tests/demo/conftest.py — mock all infrastructure, run the real test assertions.

Every fixture here shadows the session-level fixture from the root conftest.py.
The tests in tests/demo/ run the SAME assertions as the real suites, but against
mock objects that simulate a healthy 3-node Cohesity cluster.

Purpose: demonstrate the full framework during an interview WITHOUT needing
a real cluster. Run with:  pytest tests/demo/ -v
"""

from __future__ import annotations

import hashlib
import time
import uuid
from typing import Any
from unittest.mock import MagicMock

import pytest

from helix.api.models.cluster import ClusterInfo, NodeState
from helix.api.models.protection import BackupJob, ProtectionGroup, SnapshotInfo
from helix.api.models.alerts import Alert
from helix.baseline.store import JSONStore
from helix.baseline.comparator import BaselineComparator
from helix.baseline.reporter import BaselineReporter
from helix.protocols.base import ProtocolClient
from helix.tools.fio import FioResult, FioRunner, FioWorkload, FioJobSpec
from helix.tools.vdbench import VdbenchResult, VdbenchRunner, VdbenchWorkload, VdbenchSpec
from helix.collect.stats import StatsCollector
from helix.collect.artifacts import ArtifactCollector


# ─── Mock HeliosClient ────────────────────────────────────────────────────────

class MockHeliosClient:
    """
    Simulates a healthy 3-node Cohesity cluster responding to Helios API calls.
    All responses are deterministic and realistic.
    """

    def __init__(self):
        self._cluster_id = "demo-cluster-001"
        self._run_counter = 0
        self._completed_runs: set[str] = set()
        self._snapshots: dict[str, list[SnapshotInfo]] = {}

    def ping(self) -> dict:
        return {"status": "ok", "version": "7.1.0_release-20240101"}

    def get_cluster_info(self) -> ClusterInfo:
        nodes = [
            NodeState(id=f"node-{i}", ip=f"10.0.0.{i+1}", status="kHealthy",
                      role="kLeader" if i == 0 else "kFollower",
                      disk_count=12, uptime_secs=86400 * 30,
                      software_version="7.1.0_release-20240101")
            for i in range(3)
        ]
        return ClusterInfo(
            cluster_id=self._cluster_id,
            name="helix-demo-cluster",
            nodes=nodes,
        )

    def list_clusters(self) -> list[dict]:
        return [{"id": self._cluster_id, "name": "helix-demo-cluster", "ip": "10.0.0.1"}]

    def list_protection_jobs(self, cluster_id: str = "") -> list[ProtectionGroup]:
        return [
            ProtectionGroup(id=1, name="VM-Daily-Backup", environment="kVMware", is_active=True),
            ProtectionGroup(id=2, name="NAS-Hourly", environment="kPhysical", is_active=True),
        ]

    def list_alerts(self, cluster_id: str = "") -> list[Alert]:
        return [
            Alert(id="a001", severity="kInfo", message="Backup completed successfully",
                  description="Backup completed successfully",
                  timestamp_usecs=int(time.time() * 1e6)),
        ]

    def create_protection_group(self, name: str, cluster_id: str = "") -> ProtectionGroup:
        return ProtectionGroup(id=int(uuid.uuid4().int % 100000), name=name,
                               environment="kPhysical", is_active=True)

    def delete_protection_group(self, group_id: int, cluster_id: str = "",
                                delete_snapshots: bool = True) -> None:
        self._snapshots.pop(str(group_id), None)

    def trigger_backup(self, group_id: int, cluster_id: str = "") -> BackupJob:
        self._run_counter += 1
        run_id = f"run-{self._run_counter:04d}"
        self._completed_runs.add(run_id)
        return BackupJob(
            id=self._run_counter,
            run_id=run_id,
            job_id=group_id,
            status="kAccepted",
            run_type="kRegular",
            start_time_usecs=int(time.time() * 1e6),
        )

    def get_backup_run(self, run_id: str, cluster_id: str = "") -> BackupJob:
        # Always return success (mock simulates immediate completion)
        return BackupJob(
            id=1,
            status="kSuccess",
            run_type="kRegular",
            start_time_usecs=int((time.time() - 30) * 1e6),
            end_time_usecs=int(time.time() * 1e6),
            total_bytes_transferred=1024 * 1024 * 500,
        )

    def list_snapshots(self, group_id: int, cluster_id: str = "") -> list[SnapshotInfo]:
        return [
            SnapshotInfo(
                id=f"snap-{group_id}-001",
                job_id=group_id,
                started_time_usecs=int(time.time() * 1e6),
                expiry_time_usecs=int((time.time() + 86400 * 30) * 1e6),
                total_bytes_on_tier=1024 * 1024 * 500,
            )
        ]

    def restore(self, snapshot_id: str, target_dir: str, cluster_id: str = "") -> dict:
        return {"task_id": f"restore-{uuid.uuid4().hex[:8]}", "status": "kSuccess"}

    def get_protection_summary(self) -> dict:
        return {
            "protectedObjectCount": 42,
            "unprotectedObjectCount": 3,
            "protectedSizeBytes": 1024 ** 4 * 5,  # 5 TB
            "lastBackupTime": int(time.time()),
        }

    def close(self) -> None:
        pass


# ─── Mock SSHClient ───────────────────────────────────────────────────────────

class MockRemoteResult:
    def __init__(self, stdout="", stderr="", exit_code=0):
        self.stdout = stdout
        self.stderr = stderr
        self.exit_code = exit_code

    @property
    def ok(self):
        return self.exit_code == 0


class MockSSHClient:
    """Returns realistic-looking command output for cluster diagnostics."""

    def connect(self): pass
    def disconnect(self): pass

    def run(self, command: str, check: bool = True) -> MockRemoteResult:
        # Return plausible output based on command
        if "tail" in command and "iris" in command:
            return MockRemoteResult(
                stdout="I0101 00:00:01 iris started, version=7.1.0\n"
                       "I0101 00:00:02 Listening on port 27018\n"
                       "I0101 00:00:05 Cluster quorum established\n",
            )
        elif "journalctl" in command:
            return MockRemoteResult(stdout="Jan 01 00:00:01 node-1 cohesity[1234]: Service healthy\n")
        elif "dmesg" in command:
            return MockRemoteResult(stdout="[    0.000000] Linux version 5.15.0\n")
        elif "df -h" in command:
            return MockRemoteResult(
                stdout="/dev/sda1      100G   45G   55G  45% /\n"
                       "/dev/sdb1      2.0T  800G  1.2T  40% /cohesity/data\n"
            )
        elif "vmstat" in command:
            return MockRemoteResult(stdout=" 1  0  0 8192000 1024000 4096000 0 0 0 0 100 200 12  5 82  1\n")
        elif "free -m" in command:
            return MockRemoteResult(stdout="Mem:          64000      28000      12000       1000      24000      35000\n")
        elif "iostat" in command:
            return MockRemoteResult(stdout='{"sysstat":{"hosts":[{"statistics":[{"avg-cpu":{"user":12.0,"system":5.0,"iowait":1.5,"idle":81.5},"disk":[{"disk_device":"sda","r/s":250,"w/s":180,"rkB/s":62000,"wkB/s":45000,"%util":28.0}]}]}]}}')
        elif "ps aux" in command:
            return MockRemoteResult(stdout="cohesity  1234  5.0  2.1 cohesity/iris --port=27018\n")
        elif "ss -tlnp" in command:
            return MockRemoteResult(stdout="LISTEN  0  128  0.0.0.0:27018  0.0.0.0:*  users:((\"iris\",pid=1234,fd=10))\n")
        elif "/proc/net/dev" in command:
            return MockRemoteResult(stdout="  eth0:  1234567890  9876543  0  0  0  0  0  0  987654321  8765432  0  0  0  0  0  0\n")
        elif "nohup" in command:
            return MockRemoteResult(stdout="12345")
        elif "echo $!" in command:
            return MockRemoteResult(stdout="12345")
        return MockRemoteResult(stdout="OK\n")

    def get_file(self, remote_path: str, local_path: str) -> None:
        pass

    def put_file(self, local_path: str, remote_path: str) -> None:
        pass

    def read_file(self, remote_path: str) -> str:
        return ""


# ─── Mock ProtocolClient ──────────────────────────────────────────────────────

class MockProtocolClient(ProtocolClient):
    """
    In-memory file store implementing the ProtocolClient ABC.
    write_file / read_file / delete_file all operate on a shared dict.
    This lets the backup/restore test actually verify checksums.
    """
    _store: dict[str, bytes] = {}  # shared across all mock instances

    def __init__(self, protocol: str = "smb"):
        self._protocol = protocol

    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def write_file(self, remote_path: str, data: bytes) -> None:
        MockProtocolClient._store[remote_path] = data

    def read_file(self, remote_path: str) -> bytes:
        if remote_path not in MockProtocolClient._store:
            raise FileNotFoundError(f"NoSuchKey: {remote_path}")
        return MockProtocolClient._store[remote_path]

    def list_directory(self, remote_path: str) -> list[str]:
        prefix = remote_path.rstrip("/") + "/"
        return [k for k in MockProtocolClient._store if k.startswith(prefix)]

    def delete_file(self, remote_path: str) -> None:
        MockProtocolClient._store.pop(remote_path, None)

    @property
    def protocol_name(self) -> str:
        return self._protocol

    def __repr__(self):
        return f"MockProtocolClient({self._protocol})"

    def __str__(self):
        return self._protocol.upper()


# ─── Mock FioRunner ───────────────────────────────────────────────────────────

class MockFioRunner(FioRunner):
    """Returns realistic pre-computed FioResults without running fio."""

    _RESULTS = {
        FioWorkload.RANDOM_READ:    dict(read_iops=54320, read_bw_mbs=212, read_lat_p99_us=8200),
        FioWorkload.RANDOM_WRITE:   dict(write_iops=32100, write_bw_mbs=125, write_lat_p99_us=4100),
        FioWorkload.SEQUENTIAL_WRITE: dict(write_iops=820, write_bw_mbs=820, write_lat_p99_us=1200),
        FioWorkload.SEQUENTIAL_READ:  dict(read_iops=840, read_bw_mbs=840, read_lat_p99_us=980),
        FioWorkload.MIXED_OLTP: dict(read_iops=36000, write_iops=15400, read_bw_mbs=140,
                                     write_bw_mbs=60, read_lat_p99_us=6200, write_lat_p99_us=4800),
        FioWorkload.MIXED_BACKUP: dict(write_iops=640, write_bw_mbs=640, read_bw_mbs=160,
                                       read_iops=160, write_lat_p99_us=1800),
    }

    def run(self, **kwargs) -> FioResult:
        spec = kwargs.get("spec")
        if isinstance(spec, FioJobSpec):
            profile = spec.workload_profile
        else:
            profile = FioWorkload.RANDOM_READ

        data = self._RESULTS.get(profile, {})
        return FioResult(
            job_name="helix-demo",
            read_iops=float(data.get("read_iops", 0)),
            read_bw_mbs=float(data.get("read_bw_mbs", 0)),
            read_lat_p99_us=float(data.get("read_lat_p99_us", 0)),
            write_iops=float(data.get("write_iops", 0)),
            write_bw_mbs=float(data.get("write_bw_mbs", 0)),
            write_lat_p99_us=float(data.get("write_lat_p99_us", 0)),
            raw_json='{"fio version": "fio-3.35", "jobs": [{"read":{},"write":{}}]}',
        )


# ─── Mock VdbenchRunner ───────────────────────────────────────────────────────

class MockVdbenchRunner(VdbenchRunner):
    def run(self, **kwargs) -> VdbenchResult:
        spec = kwargs.get("spec")
        if isinstance(spec, VdbenchSpec) and spec.workload == VdbenchWorkload.DW:
            return VdbenchResult(iops=1600.0, throughput_mbs=410.0,
                                 avg_response_ms=2.0, p99_response_ms=8.5, workload="dw")
        return VdbenchResult(iops=33800.0, throughput_mbs=132.0,
                             avg_response_ms=0.95, p99_response_ms=3.2, workload="oltp")


# ─── Mock StatsCollector + ArtifactCollector ─────────────────────────────────

class MockStatsCollector:
    def start(self): pass
    def stop(self): pass
    def write_csv(self, path): pass
    def attach_to_allure(self, path): pass
    def get_peak_disk_util(self, node_id): return 28.0
    def get_avg_cpu_iowait(self, node_id): return 1.5


class MockArtifactCollector:
    def collect_all(self): return []


# ─── Mock FaultInjector ───────────────────────────────────────────────────────

class MockFaultInjector:
    def __init__(self):
        self._active_network_faults = []
        self._active_node_faults = []
        self._active_disk_faults = []

    def kill_node(self, node_id, process="iris"): return MagicMock()
    def partition_nodes(self, node_id, from_ips, interface=None): return MagicMock()
    def add_latency(self, node_id, delay_ms, jitter_ms=10): return MagicMock()
    def add_packet_loss(self, node_id, loss_pct): return MagicMock()
    def inject_disk_errors(self, node_id, device): return MagicMock()
    def stop_node_service(self, node_id, service): return MagicMock()
    def _get_ssh(self, node_id): return MockSSHClient()
    def heal_all(self): pass


# ─── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def helix_config():
    cfg = MagicMock()
    cfg.cluster_ip = "10.0.0.1"
    cfg.cluster_id = "demo-cluster-001"
    cfg.env = "lab"
    cfg.api_key = "DEMO_API_KEY"
    cfg.nfs_export = "/demo-nfs-export"
    cfg.smb_share = "demo-share"
    cfg.smb_user = "demouser"
    cfg.smb_password = "demopass"
    cfg.s3_bucket = "helix-demo-bucket"
    cfg.s3_access_key = "DEMO_ACCESS_KEY"
    cfg.s3_secret_key = "DEMO_SECRET_KEY"
    cfg.iscsi_target = "iqn.2024-01.com.cohesity:demo"
    cfg.iscsi_portal = "10.0.0.1"
    cfg.ssh_user = "cohesity"
    cfg.node_ips = ["10.0.0.1", "10.0.0.2", "10.0.0.3"]
    cfg.baseline_backend = "json"
    cfg.baseline_dir = "/tmp/helix_demo_baselines"
    cfg.update_baselines = False
    return cfg


@pytest.fixture(scope="session")
def helios_client():
    return MockHeliosClient()


@pytest.fixture(scope="session")
def ssh_nodes():
    return {f"node-{i}": MockSSHClient() for i in range(1, 4)}


@pytest.fixture(scope="session", autouse=True)
def verify_cluster_health(helios_client):
    """Demo: cluster is always healthy."""
    info = helios_client.get_cluster_info()
    assert info.quorum_ok, "Mock cluster should always be healthy"
    yield
    # Post-suite check
    post_info = helios_client.get_cluster_info()
    assert post_info.quorum_ok


@pytest.fixture(scope="session", autouse=True)
def stats_collector(ssh_nodes):
    collector = MockStatsCollector()
    collector.start()
    yield collector
    collector.stop()


@pytest.fixture
def artifact_collector(ssh_nodes, helios_client, helix_config):
    yield MockArtifactCollector()


@pytest.fixture(
    params=["smb", "nfs", "s3"],
    ids=["SMB", "NFS", "S3"],
)
def protocol_client(request):
    """
    Parametrized mock protocol client.
    In demo mode we skip iSCSI (requires block device on the host).
    Each test runs 3 times: [SMB] [NFS] [S3]
    """
    MockProtocolClient._store.clear()   # fresh store per test
    client = MockProtocolClient(protocol=request.param)
    client.connect()
    yield client
    client.disconnect()


@pytest.fixture(scope="module")
def clean_protection_group(helios_client, helix_config):
    group = helios_client.create_protection_group(
        name=f"helix-demo-{uuid.uuid4().hex[:6]}",
        cluster_id=helix_config.cluster_id,
    )
    yield group
    helios_client.delete_protection_group(group_id=group.id,
                                          cluster_id=helix_config.cluster_id,
                                          delete_snapshots=True)


@pytest.fixture(scope="session")
def baseline_store(helix_config):
    store = JSONStore("/tmp/helix_demo_baselines")
    return store


@pytest.fixture(scope="session")
def baseline_comparator(baseline_store):
    return BaselineComparator(baseline_store, hard_regression_pct=5.0,
                               warning_pct=2.0, min_samples=3)


@pytest.fixture(scope="session")
def baseline_reporter():
    return BaselineReporter()


@pytest.fixture
def fio_runner():
    return MockFioRunner()


@pytest.fixture
def vdbench_runner():
    return MockVdbenchRunner()


@pytest.fixture
def fault_injector(ssh_nodes, helios_client):
    injector = MockFaultInjector()
    yield injector
    injector.heal_all()


@pytest.fixture(autouse=True)
def ensure_healthy_before_chaos(helios_client):
    info = helios_client.get_cluster_info()
    assert info.quorum_ok
    yield
