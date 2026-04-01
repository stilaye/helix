# HELIX — Helios EXtensible Library for Infrastructure eXamination

> A production-grade Python pytest automation framework for Cohesity storage testing.
> Built around the **Helios management API** and the full suite of storage benchmarking tools.

---

## Why HELIX?

| Design Goal | How HELIX Achieves It |
|---|---|
| **Configurable** | `pydantic-settings` reads from `.env` + env vars + CLI flags — switch clusters without touching code |
| **Maintainable** | ABCs for protocols and tools; SLAs in one `constants.py`; Pydantic models catch API drift at build time |
| **Scalable** | `pytest-xdist -n auto` for parallel workers; SSH transparency runs tools locally or remotely; GitHub Actions matrix for nightly suites |
| **Observable** | tshark captures, `StatsCollector` (sar/iostat/vmstat), `ArtifactCollector` (Helios API + node logs on failure) — all attached to Allure |

---

## Framework Architecture

```
helix/
├── helix/
│   ├── api/            # Helios REST API client + Pydantic models
│   ├── protocols/      # ProtocolClient ABC + SMB / NFS / S3 / iSCSI
│   ├── tools/          # ToolRunner ABC + fio / vdbench / dd / specfs / fsct / virtana
│   ├── fault/          # FaultInjector: iptables, tc netem, node kill, disk errors
│   ├── baseline/       # Statistical regression detection (5% hard + 2σ soft)
│   ├── capture/        # tshark captures + SMB/NFS/S3/iSCSI protocol parsers
│   ├── collect/        # ArtifactCollector (on-failure) + StatsCollector (continuous)
│   ├── ssh/            # paramiko SSH wrapper
│   ├── utils/          # retry, checksum, wait, data_gen, logging
│   └── constants.py    # PerformanceSLA, ResilienceSLA, QualitySLA
├── tests/
│   ├── smoke/          # PR gate: cluster health, API ping, basic protocol I/O
│   ├── functional/     # Protocol correctness, backup/restore, snapshots, cluster ops
│   ├── performance/    # fio / vdbench / specFS baselines + regression detection
│   ├── chaos/          # Node kill, network partition, disk fault, capacity limits
│   └── integration/    # Cross-protocol, Helios CRUD, cloud tier
├── baselines/          # Committed JSON performance baselines
├── scripts/            # run_smoke.sh, run_regression.sh, update_baselines.py
└── .github/workflows/  # smoke.yml (PR), nightly.yml (regression matrix)
```

---

## Tools Used & How They're Utilized

### Storage Benchmarking Tools

#### `fio` — Flexible I/O Tester
**Purpose:** Generate controlled I/O workloads to benchmark storage performance.

| Parameter | What it does |
|---|---|
| `--rw=randrw --rwmixread=70` | 70/30 read/write mixed workload |
| `--bs=4k --iodepth=32` | 4KB blocks, 32 outstanding I/Os |
| `--ioengine=libaio` | Async I/O for maximum throughput |
| `--output-format=json` | Machine-parseable output |

**HELIX Integration:** `helix/tools/fio.py` — `FioRunner` accepts a `FioJobSpec` Pydantic model with a `workload_profile` enum (`SEQUENTIAL_WRITE`, `RANDOM_READ`, `MIXED_OLTP`), runs fio via subprocess (locally or via SSH), parses JSON into a typed `FioResult` model, and compares against stored baselines.

```python
runner = FioRunner(ssh_client=ssh_nodes["node-1"])
result = runner.run(
    workload_profile=FioWorkload.RANDOM_READ,
    filename="/cohesity/test-lun",
    size="10g", runtime=120, iodepth=32, bs="4k"
)
assert result.read_iops > PerformanceSLA.FIO_RAND_READ_MIN_IOPS   # >50,000 IOPS
assert result.read_lat_p99_us < PerformanceSLA.FIO_LAT_P99_MAX_MS * 1000
```

**Baselines stored in:** `baselines/fio_rand_read.json`, `baselines/fio_seq_write.json`

---

#### `vdbench` — Virtual Database Benchmark
**Purpose:** Simulate realistic database workloads (OLTP, Data Warehouse) on storage.

| Workload | Profile |
|---|---|
| OLTP | Random I/O, 8KB blocks, 90% read, high concurrency |
| Data Warehouse | Sequential I/O, 256KB blocks, full scan patterns |
| Mixed | Configurable read/write ratio, varied block sizes |

**HELIX Integration:** `helix/tools/vdbench.py` — `VdbenchRunner` generates the vdbench config file from a `VdbenchSpec` Pydantic model (no manual `.conf` files), runs via subprocess, parses `output/*.csv` into `VdbenchResult` with per-interval samples for percentile calculation.

```python
runner = VdbenchRunner()
result = runner.run(
    workload=VdbenchWorkload.OLTP,
    anchor="/mnt/nfs-test",
    iorate=5000, rdpct=90, runtime=120
)
assert result.response_time_ms_p99 < 5.0
```

---

#### `dd` — Disk Dump
**Purpose:** Simple byte-level I/O for throughput measurement and data generation.

```bash
# What HELIX runs internally:
dd if=/dev/urandom of=/mnt/test/payload bs=1M count=1000 conv=fsync oflag=direct
```

**HELIX Integration:** `helix/tools/dd.py` — `DDRunner` wraps dd, parses the `X MB/s` throughput from stderr, and returns a `DDResult(throughput_mbs, elapsed_secs, bytes_written)`. Used in `tests/performance/test_ingestion_speed.py` to measure raw backup ingestion GB/min.

---

#### `specFS` — SPEC Filesystem Benchmark
**Purpose:** Industry-standard filesystem benchmark mimicking real workloads.

| Workload | Description |
|---|---|
| `SFS-SOW` | Server Operations Workload — general file server |
| `SFS-OW` | Office Operations Workload — mixed office patterns |
| `SFS-EW` | Engineering Operations Workload — large file engineering |

**HELIX Integration:** `helix/tools/specfs.py` — `SpecFSRunner` runs each workload profile and parses throughput (ops/sec) and response time into `SpecFSResult`. Used to validate Cohesity's scale-out filesystem performance against industry standards and detect regressions across releases.

---

#### `fsct` / `fsck` — Filesystem Check Tool
**Purpose:** Verify filesystem integrity; detect and repair corruption.

```bash
fsck -n /dev/sda1    # dry run (HELIX default)
fsck -y /dev/sda1    # auto-repair (requires unmount)
```

**HELIX Integration:** `helix/tools/fsct.py` — `FsctRunner` runs over SSH on the target node. `dry_run()` returns `FsckResult(clean: bool, error_count: int, warnings: list[str])`. Used post-chaos-test to verify data integrity after node kills and network partitions.

---

#### `virtana` — Virtualization Analytics
**Purpose:** Monitor storage performance impact in virtualized environments (vSphere, Hyper-V).

**HELIX Integration:** `helix/tools/virtana.py` — `VirtanaClient` wraps the Virtana API to collect VM I/O patterns, storage latency as seen by VMs, and hotspot identification. Used in integration tests to validate that Cohesity backup jobs don't cause unacceptable latency spikes on production VMs.

---

### Protocol Testing

#### SMB (Server Message Block)
- **Port:** 445 (SMB 3.x), 139 (legacy)
- **Auth:** NTLM or Kerberos via Active Directory
- **HELIX:** `helix/protocols/smb.py` — OS-level CIFS mounts + `pysmb` fallback; tshark validates SMB 3.1.1 dialect + signing
- **Tests:** `tests/functional/test_smb.py` — auth, ACL, large file, multi-client concurrency

#### NFS (Network File System)
- **Port:** 2049 (NFS), 111 (RPC portmapper)
- **Versions:** NFSv3 (stateless), NFSv4 (stateful, ACLs)
- **HELIX:** `helix/protocols/nfs.py` — configurable mount options, stale handle recovery
- **Tests:** `tests/functional/test_nfs.py` — mount options, root_squash, stale handle recovery

#### S3 (Simple Storage Service)
- **Protocol:** HTTP/HTTPS REST API
- **HELIX:** `helix/protocols/s3.py` — boto3 wrapper targeting Cohesity's S3-compatible endpoint; multipart upload with parallel part upload
- **Tests:** `tests/functional/test_s3.py` — PUT/GET integrity, multipart, versioning, lifecycle

#### iSCSI
- **Protocol:** Block storage over TCP
- **HELIX:** `helix/protocols/iscsi.py` — `iscsiadm` wrapper for discovery/login/logout; `/sys/block` inspection for device detection
- **Tests:** `tests/functional/test_iscsi.py` — discovery, login, block I/O, disconnect

---

### Packet Capture (tshark / Wireshark)
**Purpose:** Protocol-level validation — verify what actually happened on the wire, not just what the client reported.

**HELIX Integration:** `helix/capture/tshark.py` — `TsharkCapture` context manager runs `tshark -T json -e <field>` in background, produces structured JSON (not binary pcap), stops on context exit and attaches to Allure.

```python
with TsharkCapture(interface="eth0", filter_expr="host 10.0.0.1 and tcp port 445") as cap:
    with SMBClient(config) as smb:
        smb.write_file("/share/test.bin", data)

parsed = SMBParser(cap.output_path).parse()
assert parsed.dialect == "SMB 3.1.1"
assert parsed.signing_enabled
```

Protocol-specific parsers in `helix/capture/parsers/`:
- `smb.py` — dialect, auth type (NTLM/Kerberos), session signing
- `nfs.py` — NFSv4 ops, auth flavor, root_squash behavior
- `s3.py` — HTTP methods/status codes, multipart sequence validation
- `iscsi.py` — login/logout opcodes, CHAP exchange, SCSI commands

---

### Helios Management API
**Base URL:** `https://helios.cohesity.com/irisservices/api/v1/public/`
**Auth:** Static API key — `apiKey: <key>` header (obtained via Settings → Access Management → API Keys)
**Cluster-specific calls:** Add `accessClusterId: <id>` header

**HELIX Integration:** `helix/api/client.py` — `HeliosClient` with two call modes:
- `mcm_request(method, path)` — MCM operations (cluster list, stats)
- `cluster_request(method, path, cluster_id)` — cluster-specific operations (protection jobs, snapshots)

Key endpoints used:
| Endpoint | Operation |
|---|---|
| `GET /public/mcm/clusters` | List registered clusters |
| `GET /public/nodes` | List cluster nodes + health |
| `GET /public/mcm/stats/protectionSummary` | Protection job statistics |
| `GET /public/protectionJobs` | List backup jobs (+ `accessClusterId`) |
| `POST /public/protectionJobs/run` | Trigger backup run |
| `GET /public/snapshots` | List snapshots |
| `POST /public/restore/recover` | Trigger recovery |
| `GET /public/alerts` | Cluster alerts |

---

## Quick Start

### Prerequisites

```bash
# Python 3.11+
python --version

# tshark (Wireshark CLI) — for protocol capture tests
which tshark || brew install wireshark   # macOS
# sudo apt install tshark               # Ubuntu

# fio, vdbench, dd are assumed to be on the test host or target nodes
which fio || sudo apt install fio

# Storage tools for remote nodes are run via SSH — no local install needed
```

### Installation

```bash
git clone <repo-url>
cd helix
pip install -e ".[test]"
```

### Configuration

```bash
cp .env.example .env
# Edit .env:
#   HELIOS_API_KEY=your-api-key-from-helios-ui
#   CLUSTER_IP=10.0.0.100
#   CLUSTER_ID=1234567890
#   SSH_KEY_PATH=~/.ssh/id_rsa
#   SSH_USERNAME=cohesity
```

### Running Tests

```bash
# Smoke tests (PR gate, <10 min)
pytest -m smoke --cluster-ip=10.0.0.100

# Full regression (nightly)
pytest -m "regression or perf or chaos" -n 4 --cluster-ip=10.0.0.100

# Performance only — compare against baselines
pytest -m perf --cluster-ip=10.0.0.100

# Performance — update baselines after intentional change
pytest -m perf --update-baselines --cluster-ip=10.0.0.100

# Chaos only (destructive — requires --env=lab)
pytest -m "chaos and destructive" --env=lab --cluster-ip=10.0.0.100

# Single protocol suite
pytest -m "nfs" tests/functional/ --cluster-ip=10.0.0.100

# View Allure report
allure serve allure-results/
```

### Using Shell Scripts

```bash
scripts/run_smoke.sh         # Smoke gate
scripts/run_regression.sh    # Full nightly suite
scripts/run_perf.sh          # Performance + baseline update
scripts/collect_logs.sh      # SSH pull all node logs manually
```

---

## Fixture Setup & Teardown

| Scope | Fixture | Setup | Teardown |
|---|---|---|---|
| **Session** | `verify_cluster_health` *(autouse)* | Assert quorum OK; abort run if cluster down | Post-run health check — detects test contamination |
| **Session** | `helios_client` | Authenticate to Helios; verify API ping | Close HTTP session |
| **Session** | `ssh_nodes` | Open SSH connections to all nodes | Close all connections |
| **Session** | `stats_collector` *(autouse)* | Start `sar/iostat/vmstat` background sampling on all nodes | Stop, write `host_stats.csv`, attach to Allure |
| **Module** | `clean_protection_group` | Create fresh protection group `helix-test-<uuid>` | Delete group + all snapshots (`force=True`) |
| **Module** | `test_view` | Create NAS view/share for protocol tests | Delete view + test data |
| **Function** | `protocol_client` *(params)* | Connect/mount (`cifs`, `nfs4`, boto3, iscsiadm) | Disconnect/unmount always (even on failure) |
| **Function** | `test_data_dir` | Populate `tmp_path` with known-checksum files | Auto-cleaned by pytest |
| **Function** | `fault_injector` | Initialize `FaultInjector` | `heal_all()` — remove iptables rules, restart services |
| **Function** | `packet_capture` | `TsharkCapture.start()` | Stop + attach `.json` to Allure; parse protocol frames |
| **Function** | `artifact_collector` | Register `pytest_runtest_makereport` hook | **On failure:** SSH pull iris/bridge logs + Helios API events/alerts |

---

## Performance Baselines

Baselines are committed JSON files in `baselines/`:

```json
{
  "schema_version": "1.1",
  "metric": "fio_rand_read_iops",
  "unit": "iops",
  "statistical": {
    "mean": 52400.0,
    "p50": 52100.0,
    "stddev": 800.0,
    "sample_count": 5
  },
  "raw_samples": [51200, 52400, 53100, 51900, 53400],
  "workload_params": {"rw": "randread", "bs": "4k", "iodepth": 32}
}
```

**Regression triggers if:**
1. `delta_pct < -5%` (hard threshold)
2. `current < mean - 2 × stddev` (statistical outlier)

**Warn (non-failing):** `-5% < delta < -2%`

**Update baselines** after intentional performance change:
```bash
pytest -m perf --update-baselines --cluster-ip=$IP
```

---

## CI/CD Integration

### GitHub Actions

| Workflow | Trigger | Suites |
|---|---|---|
| `smoke.yml` | Every PR | `smoke` markers — must pass before merge |
| `nightly.yml` | Daily 2am UTC | `functional`, `perf`, `chaos` in parallel matrix |

### Pytest Markers

```bash
-m smoke          # PR gate (<10 min)
-m regression     # Full functional suite
-m perf           # Performance baseline tests
-m chaos          # Fault injection tests
-m integration    # Cross-protocol E2E
-m smb            # SMB-specific tests
-m nfs            # NFS-specific tests
-m s3             # S3-specific tests
-m iscsi          # iSCSI-specific tests
-m destructive    # Tests that kill nodes (requires --env=lab)
-m slow           # Tests >5 min
```

---

## Performance SLAs

| Metric | Target |
|---|---|
| Sequential Write (backup ingestion) | > 500 MB/s |
| Random Read (restore) | > 50,000 IOPS @ 4KB |
| Latency p99 (normal ops) | < 10 ms |
| Node Failover Time | < 30 seconds |
| Backup 1TB completion | < 5 hours |
| Restore 100GB | < 1 hour |
| Test Flakiness Rate | < 2% |
| Automation Coverage | > 85% of test plan |
| Regression Detection Threshold | > 5% deviation |
| CI Smoke Duration | < 10 minutes |

---

## Project Structure Reference

```
helix/
├── helix/
│   ├── api/
│   │   ├── client.py           # HeliosClient: mcm_request() + cluster_request()
│   │   ├── auth.py             # APIKeyAuth: inject apiKey + accessClusterId headers
│   │   ├── endpoints.py        # URL path constants
│   │   └── models/
│   │       ├── cluster.py      # ClusterInfo, NodeState, QuorumStatus
│   │       ├── protection.py   # ProtectionGroup, BackupJob, SnapshotInfo
│   │       ├── storage.py      # VolumeInfo, ShareConfig, BucketPolicy
│   │       └── alerts.py       # Alert, Event
│   ├── protocols/
│   │   ├── base.py             # ProtocolClient ABC
│   │   ├── smb.py              # SMBClient
│   │   ├── nfs.py              # NFSClient
│   │   ├── s3.py               # S3Client
│   │   └── iscsi.py            # iSCSIClient
│   ├── tools/
│   │   ├── base.py             # ToolRunner ABC (SSH transparent)
│   │   ├── fio.py              # FioRunner: FioJobSpec → FioResult
│   │   ├── vdbench.py          # VdbenchRunner: VdbenchSpec → VdbenchResult
│   │   ├── dd.py               # DDRunner: DDResult(throughput_mbs)
│   │   ├── specfs.py           # SpecFSRunner: SFS-SOW/OW/EW
│   │   ├── fsct.py             # FsctRunner: dry_run() + repair()
│   │   └── virtana.py          # VirtanaClient: VM I/O + hotspots
│   ├── fault/
│   │   ├── injector.py         # FaultInjector facade
│   │   ├── network.py          # NetworkFault: iptables + tc netem (context manager)
│   │   ├── node.py             # NodeFault: SIGKILL/SIGSTOP/restart
│   │   └── disk.py             # DiskFault: dm-error, bad blocks
│   ├── baseline/
│   │   ├── store.py            # JSONStore (dev) + SQLiteStore (CI)
│   │   ├── comparator.py       # 5% hard + 2σ soft regression
│   │   └── reporter.py         # Allure attachment builder
│   ├── capture/
│   │   ├── tshark.py           # TsharkCapture context manager
│   │   └── parsers/
│   │       ├── smb.py          # SMBParser
│   │       ├── nfs.py          # NFSParser
│   │       ├── s3.py           # S3Parser
│   │       └── iscsi.py        # iSCSIParser
│   ├── collect/
│   │   ├── artifacts.py        # ArtifactCollector (on-failure hook)
│   │   └── stats.py            # StatsCollector (continuous background)
│   ├── ssh/
│   │   └── remote.py           # SSHClient (paramiko)
│   ├── constants.py            # SLA constants
│   └── utils/
│       ├── retry.py            # @retry (tenacity)
│       ├── checksum.py         # md5/sha256/xxhash
│       ├── wait.py             # poll_until / wait_for_condition
│       ├── data_gen.py         # DataGenerator
│       └── logging.py          # Structured logging
├── tests/
│   ├── conftest.py             # protocol_client fixture (params=["smb","nfs","s3","iscsi"])
│   ├── smoke/
│   ├── functional/
│   ├── performance/
│   ├── chaos/
│   └── integration/
├── baselines/
├── scripts/
└── .github/workflows/
```

---

## Resources

| Resource | URL |
|---|---|
| Cohesity Developer Portal | https://developer.cohesity.com/helios-api.html |
| Helios Getting Started Guide | https://developer.cohesity.com/docs/helios-getting-started |
| Cohesity Postman Workspace | Search "Cohesity Public Workspace" on Postman |
| Helios API Reference (V1) | https://developer.cohesity.com/apidocs/helios/ |
| fio Documentation | https://fio.readthedocs.io/ |
| vdbench User Guide | https://www.oracle.com/technetwork/server-storage/vdbench-downloads-1901681.html |
| SPEC SFS Benchmark | https://www.spec.org/sfs2014/ |
| Wireshark / tshark | https://www.wireshark.org/docs/man-pages/tshark.html |
| Allure Framework | https://allurereport.org/docs/pytest/ |
| pytest Documentation | https://docs.pytest.org/ |
| Pydantic v2 | https://docs.pydantic.dev/latest/ |
| boto3 (S3) | https://boto3.amazonaws.com/v1/documentation/api/latest/index.html |
| paramiko (SSH) | https://www.paramiko.org/ |
| tenacity (retry) | https://tenacity.readthedocs.io/ |
| pytest-xdist (parallel) | https://pytest-xdist.readthedocs.io/ |

---

## Contact & API Support

For Cohesity API/SDK questions: **cohesity-api-sdks@cohesity.com**

---

*HELIX — Built for the Cohesity SDET Filesystems role (R02604)*
