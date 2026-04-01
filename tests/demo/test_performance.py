"""
Demo: performance tests with baseline regression detection.

Uses MockFioRunner / MockVdbenchRunner that return realistic pre-computed results.
The baseline comparator, statistical regression detection, and SLA assertions
all execute against real code — only the I/O tool is mocked.

The interviewer sees:
  PASSED  test_random_read_iops        (54,320 IOPS ≥ 50,000 SLA)
  PASSED  test_sequential_write_mbs    (820 MB/s ≥ 500 MB/s SLA)
  PASSED  test_oltp_latency            (P99 3.2ms ≤ 5ms SLA)

Run:  pytest tests/demo/test_performance.py -v -s
"""

from __future__ import annotations

import pytest

from helix.tools.fio import FioWorkload, FioJobSpec
from helix.tools.vdbench import VdbenchWorkload, VdbenchSpec
from helix.constants import PerformanceSLA

pytestmark = [pytest.mark.perf, pytest.mark.regression]


class TestFioPerformance:

    def test_random_read_iops(self, fio_runner, baseline_comparator, baseline_reporter):
        """
        Random read IOPS must exceed 50,000 (Cohesity C3000 SLA).
        fio config: 4K block size, iodepth=32, 4 jobs.
        """
        spec = FioJobSpec(workload_profile=FioWorkload.RANDOM_READ,
                          filename="/mnt/nfs/fio_rand_read.dat", size="10g", runtime=60)

        result = fio_runner.run(spec=spec)

        print(f"\n  Random Read IOPS:  {result.read_iops:,.0f}")
        print(f"  Read P99 latency:  {result.read_lat_p99_us/1000:.2f}ms")
        print(f"  SLA minimum:       {PerformanceSLA.FIO_RAND_READ_MIN_IOPS:,} IOPS")

        comparison = baseline_comparator.compare(
            "demo_fio_rand_read_iops", result.read_iops, unit=" IOPS"
        )
        lat_comparison = baseline_comparator.compare(
            "demo_fio_rand_read_p99_ms", result.read_lat_p99_us / 1000,
            unit="ms", higher_is_better=False,
        )
        baseline_reporter.print_summary([comparison, lat_comparison])
        baseline_reporter.assert_no_regressions([comparison, lat_comparison])

        assert result.read_iops >= PerformanceSLA.FIO_RAND_READ_MIN_IOPS, (
            f"Random read {result.read_iops:,.0f} IOPS < "
            f"{PerformanceSLA.FIO_RAND_READ_MIN_IOPS:,} IOPS SLA"
        )
        assert result.read_lat_p99_us / 1000 <= PerformanceSLA.FIO_LAT_P99_MAX_MS, (
            f"P99 {result.read_lat_p99_us/1000:.2f}ms > {PerformanceSLA.FIO_LAT_P99_MAX_MS}ms SLA"
        )

    def test_sequential_write_throughput(self, fio_runner, baseline_comparator, baseline_reporter):
        """
        Sequential write throughput must exceed 500 MB/s (backup ingestion SLA).
        fio config: 1MB block size, iodepth=16, 4 jobs.
        """
        spec = FioJobSpec(workload_profile=FioWorkload.SEQUENTIAL_WRITE,
                          filename="/mnt/nfs/fio_seq_write.dat", size="20g", runtime=60)

        result = fio_runner.run(spec=spec)

        print(f"\n  Sequential Write:  {result.write_bw_mbs:.1f} MB/s")
        print(f"  SLA minimum:       {PerformanceSLA.FIO_SEQ_WRITE_MIN_MBS} MB/s")

        comparison = baseline_comparator.compare(
            "demo_fio_seq_write_mbs", result.write_bw_mbs, unit=" MB/s"
        )
        baseline_reporter.print_summary([comparison])
        baseline_reporter.assert_no_regressions([comparison])

        assert result.write_bw_mbs >= PerformanceSLA.FIO_SEQ_WRITE_MIN_MBS, (
            f"Sequential write {result.write_bw_mbs:.1f} MB/s < "
            f"{PerformanceSLA.FIO_SEQ_WRITE_MIN_MBS} MB/s SLA"
        )

    def test_random_write_iops(self, fio_runner, baseline_comparator, baseline_reporter):
        """Random write IOPS must exceed 30,000."""
        spec = FioJobSpec(workload_profile=FioWorkload.RANDOM_WRITE,
                          filename="/mnt/nfs/fio_rand_write.dat", size="10g", runtime=60)

        result = fio_runner.run(spec=spec)

        print(f"\n  Random Write IOPS: {result.write_iops:,.0f}")

        comparison = baseline_comparator.compare(
            "demo_fio_rand_write_iops", result.write_iops, unit=" IOPS"
        )
        baseline_reporter.print_summary([comparison])
        baseline_reporter.assert_no_regressions([comparison])

        assert result.write_iops >= 30_000, (
            f"Random write {result.write_iops:,.0f} IOPS < 30,000 SLA"
        )

    def test_oltp_mixed_workload(self, fio_runner, baseline_comparator, baseline_reporter):
        """OLTP: 70/30 read/write mix must sustain acceptable IOPS and latency."""
        spec = FioJobSpec(workload_profile=FioWorkload.MIXED_OLTP,
                          filename="/mnt/nfs/fio_oltp.dat", size="10g", runtime=60)

        result = fio_runner.run(spec=spec)

        print(f"\n  OLTP Read IOPS:  {result.read_iops:,.0f}")
        print(f"  OLTP Write IOPS: {result.write_iops:,.0f}")

        comparisons = baseline_comparator.compare_batch({
            "demo_oltp_read_iops":  (result.read_iops, " IOPS", True),
            "demo_oltp_write_iops": (result.write_iops, " IOPS", True),
            "demo_oltp_read_p99":   (result.read_lat_p99_us / 1000, "ms", False),
        })
        baseline_reporter.print_summary(comparisons)
        baseline_reporter.assert_no_regressions(comparisons)

        total_iops = result.read_iops + result.write_iops
        assert total_iops >= 40_000, f"OLTP total IOPS {total_iops:,.0f} < 40,000 SLA"


class TestVdbenchPerformance:

    def test_oltp_iops_and_latency(self, vdbench_runner, baseline_comparator, baseline_reporter):
        """
        Vdbench OLTP: enterprise DB workload — IOPS and P99 latency.
        32 threads, 4KB, 90% read / 10% write.
        """
        spec = VdbenchSpec(workload=VdbenchWorkload.OLTP, anchor="/mnt/nfs/vdbench",
                           elapsed=60, warmup=10, threads=32)

        result = vdbench_runner.run(spec=spec)

        print(f"\n  Vdbench IOPS:    {result.iops:,.0f}")
        print(f"  Vdbench P99:     {result.p99_response_ms:.2f}ms")
        print(f"  Throughput:      {result.throughput_mbs:.1f} MB/s")

        comparisons = baseline_comparator.compare_batch({
            "demo_vdbench_oltp_iops": (result.iops, " IOPS", True),
            "demo_vdbench_oltp_p99":  (result.p99_response_ms, "ms", False),
        })
        baseline_reporter.print_summary(comparisons)
        baseline_reporter.assert_no_regressions(comparisons)

        assert result.iops >= 30_000, f"Vdbench OLTP {result.iops:,.0f} IOPS < 30,000 SLA"
        assert result.p99_response_ms <= 5.0, (
            f"Vdbench P99 {result.p99_response_ms:.2f}ms > 5ms SLA"
        )
