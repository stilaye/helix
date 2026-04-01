"""
SLA constants for HELIX — single source of truth for all performance, resilience, and quality targets.
Change a value here and every test that imports it picks up the update automatically.
"""


class PerformanceSLA:
    """Storage performance targets derived from Cohesity production specifications."""

    # Sequential I/O (backup ingestion path)
    SEQ_WRITE_MIN_MBS: float = 500.0        # MB/s — backup ingestion
    SEQ_READ_MIN_MBS: float = 400.0         # MB/s — restore throughput

    # Random I/O (metadata-heavy workloads)
    RAND_READ_MIN_IOPS: float = 50_000.0    # IOPS @ 4KB block
    RAND_WRITE_MIN_IOPS: float = 20_000.0   # IOPS @ 4KB block

    # Latency
    LAT_P99_MAX_MS: float = 10.0            # ms — p99 completion latency
    LAT_P95_MAX_MS: float = 5.0             # ms — p95 completion latency
    LAT_P50_MAX_MS: float = 1.0             # ms — median latency

    # Deduplication
    DEDUP_RATIO_MIN: float = 10.0           # 10:1 minimum

    # Aliases used in tests
    FIO_SEQ_WRITE_MIN_MBS = SEQ_WRITE_MIN_MBS
    FIO_RAND_READ_MIN_IOPS = RAND_READ_MIN_IOPS
    FIO_LAT_P99_MAX_MS = LAT_P99_MAX_MS


class BackupSLA:
    """Backup and recovery operation time targets."""

    BACKUP_1TB_MAX_HOURS: float = 5.0       # hours — 1TB full backup
    RESTORE_100GB_MAX_HOURS: float = 1.0    # hours — 100GB restore
    RPO_MAX_HOURS: float = 1.0              # Recovery Point Objective
    RTO_MAX_HOURS: float = 4.0              # Recovery Time Objective

    SNAPSHOT_CREATE_MAX_SECS: float = 30.0  # seconds
    SNAPSHOT_DELETE_MAX_SECS: float = 60.0  # seconds


class ResilienceSLA:
    """Cluster resilience and fault tolerance targets."""

    NODE_FAILOVER_MAX_SECS: float = 30.0            # seconds — leader election
    DATA_RE_REPLICATION_MAX_HOURS: float = 24.0     # hours — after node loss
    SPLIT_BRAIN_PREVENTION_RATE: float = 1.0        # 100% — quorum-based
    NDU_DOWNTIME_SECS: float = 0.0                  # Non-disruptive upgrade

    QUORUM_RECOVERY_MAX_SECS: float = 30.0          # after partition healed
    NODE_REJOIN_MAX_SECS: float = 120.0             # node comes back online


class QualitySLA:
    """Test quality and CI pipeline targets."""

    PERF_REGRESSION_THRESHOLD: float = 0.05        # 5% deviation triggers failure
    PERF_WARNING_THRESHOLD: float = 0.02            # 2% triggers warning (non-failing)
    PERF_REGRESSION_SIGMA: float = 2.0              # 2σ statistical outlier detection

    TARGET_FLAKINESS_RATE: float = 0.02             # <2% flakiness
    AUTOMATION_COVERAGE_TARGET: float = 0.85        # >85% test plan coverage

    SMOKE_MAX_DURATION_MINS: float = 10.0           # minutes
    REGRESSION_MAX_DURATION_HOURS: float = 2.0      # hours (smoke only)
    NIGHTLY_MAX_DURATION_HOURS: float = 6.0         # hours (full suite)

    BASELINE_SAMPLE_COUNT: int = 5                  # rolling window for mean/stddev


class ProtocolPort:
    """Well-known port numbers for storage protocols."""

    SMB = 445
    SMB_LEGACY = 139
    NFS = 2049
    NFS_RPC = 111
    ISCSI = 3260
    S3_HTTP = 80
    S3_HTTPS = 443


class HeliosEndpoint:
    """Helios REST API path constants. Avoids magic strings scattered across the codebase."""

    BASE_URL = "https://helios.cohesity.com/irisservices/api/v1/public"

    # MCM (multi-cluster management) — no accessClusterId needed
    MCM_CLUSTERS = "/mcm/clusters"
    MCM_STATS_PROTECTION = "/mcm/stats/protectionSummary"
    MCM_ALERTS = "/mcm/alerts"

    # Cluster-specific — requires accessClusterId header
    NODES = "/nodes"
    PROTECTION_JOBS = "/protectionJobs"
    PROTECTION_JOBS_RUN = "/protectionJobs/run"
    SNAPSHOTS = "/snapshots"
    RESTORE_RECOVER = "/restore/recover"
    VIEWS = "/views"
    EXTERNAL_TARGETS = "/externalTargets"
