"""
Performance: sequential I/O via fio — backup and restore throughput.

Targets:
  - Sequential write: > 500 MB/s (backup ingestion rate)
  - Sequential read:  > 500 MB/s (restore / scan rate)

Sequential throughput is the primary bottleneck for backup ingestion jobs.
These benchmarks directly validate that hardware and network meet SLA.
"""

from __future__ import annotations

import allure
import pytest

from helix.tools.fio import FioRunner, FioWorkload, FioJobSpec
from helix.constants import PerformanceSLA

pytestmark = [pytest.mark.perf, pytest.mark.regression, pytest.mark.slow]


@allure.suite("Performance")
@allure.feature("FIO Sequential I/O")
class TestFioSequentialIO:

    @allure.title("Sequential write throughput >= 500 MB/s")
    @pytest.mark.timeout(300)
    def test_sequential_write_throughput(
        self,
        fio_runner: FioRunner,
        baseline_comparator,
        baseline_reporter,
        helix_config,
    ):
        """
        Sequential write: simulates backup ingestion (large streaming writes).
        FioWorkload.SEQUENTIAL_WRITE: rw=write, bs=1M, iodepth=8, numjobs=4
        """
        spec = FioJobSpec(
            workload_profile=FioWorkload.SEQUENTIAL_WRITE,
            target_path=f"{helix_config.nfs_export}/fio_seq_write.dat",
            size_gb=20,
            runtime_secs=60,
        )

        with allure.step("Run fio sequential write benchmark (60s)"):
            result = fio_runner.run(spec=spec)

        allure.attach(
            f"Throughput: {result.write_bw_mbs:.1f} MB/s\n"
            f"IOPS: {result.write_iops:,.0f}\n"
            f"P99 latency: {result.write_lat_p99_us / 1000:.2f}ms",
            name="fio_seq_write_results.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

        comparison = baseline_comparator.compare(
            "fio_seq_write_mbs", result.write_bw_mbs, unit=" MB/s"
        )
        baseline_reporter.attach_to_allure([comparison], test_name="test_sequential_write_throughput")
        baseline_reporter.print_summary([comparison])
        baseline_reporter.assert_no_regressions([comparison])

        assert result.write_bw_mbs >= PerformanceSLA.FIO_SEQ_WRITE_MIN_MBS, (
            f"Sequential write {result.write_bw_mbs:.1f} MB/s < "
            f"SLA minimum {PerformanceSLA.FIO_SEQ_WRITE_MIN_MBS} MB/s"
        )

    @allure.title("Sequential read throughput >= 500 MB/s")
    @pytest.mark.timeout(300)
    def test_sequential_read_throughput(
        self,
        fio_runner: FioRunner,
        baseline_comparator,
        baseline_reporter,
        helix_config,
    ):
        """Sequential read: simulates restore / full-scan operations."""
        spec = FioJobSpec(
            workload_profile=FioWorkload.SEQUENTIAL_READ,
            target_path=f"{helix_config.nfs_export}/fio_seq_read.dat",
            size_gb=20,
            runtime_secs=60,
        )

        with allure.step("Run fio sequential read benchmark (60s)"):
            result = fio_runner.run(spec=spec)

        allure.attach(
            f"Throughput: {result.read_bw_mbs:.1f} MB/s\n"
            f"IOPS: {result.read_iops:,.0f}",
            name="fio_seq_read_results.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

        comparison = baseline_comparator.compare(
            "fio_seq_read_mbs", result.read_bw_mbs, unit=" MB/s"
        )
        baseline_reporter.attach_to_allure([comparison], test_name="test_sequential_read_throughput")
        baseline_reporter.print_summary([comparison])
        baseline_reporter.assert_no_regressions([comparison])

        assert result.read_bw_mbs >= PerformanceSLA.FIO_SEQ_WRITE_MIN_MBS, (
            f"Sequential read {result.read_bw_mbs:.1f} MB/s < "
            f"SLA minimum {PerformanceSLA.FIO_SEQ_WRITE_MIN_MBS} MB/s"
        )

    @allure.title("Backup-pattern workload: large sequential write + read mix")
    @pytest.mark.timeout(300)
    def test_backup_pattern(
        self,
        fio_runner: FioRunner,
        baseline_comparator,
        baseline_reporter,
        helix_config,
    ):
        """
        MIXED_BACKUP profile: large block sequential, models backup agent writing
        while protection job runs concurrently.
        rw=rw, bs=512k, iodepth=8, numjobs=4, rwmixread=20
        """
        spec = FioJobSpec(
            workload_profile=FioWorkload.MIXED_BACKUP,
            target_path=f"{helix_config.nfs_export}/fio_backup.dat",
            size_gb=20,
            runtime_secs=60,
        )

        with allure.step("Run fio backup-pattern benchmark (60s)"):
            result = fio_runner.run(spec=spec)

        allure.attach(
            f"Read: {result.read_bw_mbs:.1f} MB/s | Write: {result.write_bw_mbs:.1f} MB/s\n"
            f"Combined: {result.read_bw_mbs + result.write_bw_mbs:.1f} MB/s",
            name="fio_backup_pattern_results.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

        comparisons = baseline_comparator.compare_batch({
            "fio_backup_write_mbs": (result.write_bw_mbs, " MB/s", True),
            "fio_backup_read_mbs": (result.read_bw_mbs, " MB/s", True),
        })
        baseline_reporter.attach_to_allure(comparisons, test_name="test_backup_pattern")
        baseline_reporter.print_summary(comparisons)
        baseline_reporter.assert_no_regressions(comparisons)
