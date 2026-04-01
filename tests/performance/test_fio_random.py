"""
Performance: random I/O via fio — IOPS, latency, and bandwidth benchmarks.

Targets:
  - Random read:  > 50,000 IOPS @ 4KB block size
  - Random write: > 30,000 IOPS @ 4KB block size
  - P99 latency:  < 10ms for random reads

All results compared against stored baselines with:
  - 5% hard regression threshold (test fails)
  - 2σ statistical outlier detection (catches one-off noise vs. real regression)

These targets are based on Cohesity C3000/C4000 hardware specs.
"""

from __future__ import annotations

import allure
import pytest

from helix.tools.fio import FioRunner, FioWorkload, FioJobSpec
from helix.constants import PerformanceSLA

pytestmark = [pytest.mark.perf, pytest.mark.regression, pytest.mark.slow]


@allure.suite("Performance")
@allure.feature("FIO Random I/O")
class TestFioRandomIO:

    @allure.title("Random read IOPS >= 50,000 (4KB blocks, QD=32)")
    @pytest.mark.timeout(300)
    def test_random_read_iops(
        self,
        fio_runner: FioRunner,
        baseline_comparator,
        baseline_reporter,
        helix_config,
    ):
        """
        Benchmark: random read IOPS on Cohesity NFS mount.
        FioWorkload.RANDOM_READ expands to: rw=randread, bs=4k, iodepth=32, numjobs=4
        """
        spec = FioJobSpec(
            workload_profile=FioWorkload.RANDOM_READ,
            target_path=f"{helix_config.nfs_export}/fio_rand_read.dat",
            size_gb=10,
            runtime_secs=60,
        )

        with allure.step("Run fio random read benchmark (60s)"):
            result = fio_runner.run(spec=spec)

        allure.attach(
            f"IOPS: {result.read_iops:,.0f}\n"
            f"Throughput: {result.read_bw_mbs:.1f} MB/s\n"
            f"P99 latency: {result.read_lat_p99_us / 1000:.2f}ms",
            name="fio_random_read_results.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

        # Baseline comparison
        comparison = baseline_comparator.compare(
            "fio_rand_read_iops", result.read_iops, unit=" IOPS"
        )
        lat_comparison = baseline_comparator.compare(
            "fio_rand_read_p99_lat_ms",
            result.read_lat_p99_us / 1000,
            unit="ms",
            higher_is_better=False,
        )
        baseline_reporter.attach_to_allure(
            [comparison, lat_comparison],
            test_name="test_random_read_iops",
        )
        baseline_reporter.print_summary([comparison, lat_comparison])
        baseline_reporter.assert_no_regressions([comparison, lat_comparison])

        # Absolute SLA check (regardless of baseline)
        assert result.read_iops >= PerformanceSLA.FIO_RAND_READ_MIN_IOPS, (
            f"Random read IOPS {result.read_iops:,.0f} < "
            f"SLA minimum {PerformanceSLA.FIO_RAND_READ_MIN_IOPS:,.0f}"
        )
        assert result.read_lat_p99_us / 1000 <= PerformanceSLA.FIO_LAT_P99_MAX_MS, (
            f"P99 read latency {result.read_lat_p99_us / 1000:.2f}ms > "
            f"SLA maximum {PerformanceSLA.FIO_LAT_P99_MAX_MS}ms"
        )

    @allure.title("Random write IOPS >= 30,000 (4KB blocks, QD=32)")
    @pytest.mark.timeout(300)
    def test_random_write_iops(
        self,
        fio_runner: FioRunner,
        baseline_comparator,
        baseline_reporter,
        helix_config,
    ):
        """Benchmark: random write IOPS."""
        spec = FioJobSpec(
            workload_profile=FioWorkload.RANDOM_WRITE,
            target_path=f"{helix_config.nfs_export}/fio_rand_write.dat",
            size_gb=10,
            runtime_secs=60,
        )

        with allure.step("Run fio random write benchmark (60s)"):
            result = fio_runner.run(spec=spec)

        allure.attach(
            f"IOPS: {result.write_iops:,.0f}\n"
            f"Throughput: {result.write_bw_mbs:.1f} MB/s\n"
            f"P99 latency: {result.write_lat_p99_us / 1000:.2f}ms",
            name="fio_random_write_results.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

        comparison = baseline_comparator.compare(
            "fio_rand_write_iops", result.write_iops, unit=" IOPS"
        )
        baseline_reporter.attach_to_allure([comparison], test_name="test_random_write_iops")
        baseline_reporter.print_summary([comparison])
        baseline_reporter.assert_no_regressions([comparison])

        assert result.write_iops >= 30_000, (
            f"Random write IOPS {result.write_iops:,.0f} < 30,000 minimum"
        )

    @allure.title("OLTP workload (70/30 read/write mix) maintains acceptable latency")
    @pytest.mark.timeout(300)
    def test_mixed_oltp_workload(
        self,
        fio_runner: FioRunner,
        baseline_comparator,
        baseline_reporter,
        helix_config,
    ):
        """
        OLTP simulation: 70% random read, 30% random write, 4KB blocks.
        Models backup agent I/O pattern during active database protection.
        """
        spec = FioJobSpec(
            workload_profile=FioWorkload.MIXED_OLTP,
            target_path=f"{helix_config.nfs_export}/fio_oltp.dat",
            size_gb=10,
            runtime_secs=60,
        )

        with allure.step("Run fio OLTP mixed workload benchmark (60s)"):
            result = fio_runner.run(spec=spec)

        allure.attach(
            f"Read IOPS: {result.read_iops:,.0f} | Write IOPS: {result.write_iops:,.0f}\n"
            f"Read P99: {result.read_lat_p99_us/1000:.2f}ms | Write P99: {result.write_lat_p99_us/1000:.2f}ms",
            name="fio_oltp_results.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

        comparisons = baseline_comparator.compare_batch({
            "fio_oltp_read_iops": (result.read_iops, " IOPS", True),
            "fio_oltp_write_iops": (result.write_iops, " IOPS", True),
            "fio_oltp_read_p99_ms": (result.read_lat_p99_us / 1000, "ms", False),
        })
        baseline_reporter.attach_to_allure(comparisons, test_name="test_mixed_oltp_workload")
        baseline_reporter.print_summary(comparisons)
        baseline_reporter.assert_no_regressions(comparisons)
