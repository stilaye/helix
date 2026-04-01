"""
Performance: vdbench OLTP workload — enterprise database I/O simulation.

Vdbench is the industry-standard storage benchmark used by Oracle, EMC, and
Cohesity engineering for enterprise workload characterization. Unlike fio,
vdbench models multi-thread VM I/O patterns more accurately for database workloads.

OLTP workload characteristics:
  - Small block (4KB-8KB)
  - High concurrency (32-64 threads)
  - 70% read / 30% write
  - Random access pattern
  - Many short operations (latency-sensitive, not throughput-sensitive)

Targets:
  - Throughput: > 30,000 IOPS aggregate
  - P99 response time: < 5ms
"""

from __future__ import annotations

import allure
import pytest

from helix.tools.vdbench import VdbenchRunner, VdbenchWorkload, VdbenchSpec
from helix.constants import PerformanceSLA

pytestmark = [pytest.mark.perf, pytest.mark.regression, pytest.mark.slow]


@allure.suite("Performance")
@allure.feature("Vdbench OLTP")
class TestVdbenchOLTP:

    @allure.title("Vdbench OLTP: > 30,000 IOPS with < 5ms P99 latency")
    @pytest.mark.timeout(600)
    def test_oltp_iops_and_latency(
        self,
        vdbench_runner: VdbenchRunner,
        baseline_comparator,
        baseline_reporter,
        helix_config,
    ):
        """
        OLTP workload simulation via vdbench.
        Validates Cohesity meets enterprise database backup I/O requirements.
        """
        spec = VdbenchSpec(
            workload=VdbenchWorkload.OLTP,
            target_path=f"{helix_config.nfs_export}/vdbench_oltp",
            data_size_gb=50,
            threads=32,
            warmup_secs=30,
            runtime_secs=120,
        )

        with allure.step("Run vdbench OLTP (120s + 30s warmup)"):
            result = vdbench_runner.run(spec=spec)

        allure.attach(
            f"IOPS: {result.iops:,.0f}\n"
            f"Throughput: {result.throughput_mbs:.1f} MB/s\n"
            f"Avg response time: {result.avg_response_ms:.2f}ms\n"
            f"P99 response time: {result.p99_response_ms:.2f}ms",
            name="vdbench_oltp_results.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

        comparisons = baseline_comparator.compare_batch({
            "vdbench_oltp_iops": (result.iops, " IOPS", True),
            "vdbench_oltp_p99_ms": (result.p99_response_ms, "ms", False),
        })
        baseline_reporter.attach_to_allure(comparisons, test_name="test_oltp_iops_and_latency")
        baseline_reporter.print_summary(comparisons)
        baseline_reporter.assert_no_regressions(comparisons)

        assert result.iops >= 30_000, (
            f"OLTP IOPS {result.iops:,.0f} < 30,000 SLA minimum"
        )
        assert result.p99_response_ms <= 5.0, (
            f"OLTP P99 latency {result.p99_response_ms:.2f}ms > 5ms SLA maximum"
        )

    @allure.title("Vdbench DW (data warehouse): sequential large-block throughput")
    @pytest.mark.timeout(600)
    def test_dw_throughput(
        self,
        vdbench_runner: VdbenchRunner,
        baseline_comparator,
        baseline_reporter,
        helix_config,
    ):
        """
        Data warehouse workload: large sequential blocks, high throughput.
        Models analytics/reporting workloads that do full table scans.
        Block size: 256KB-1MB, sequential pattern.
        """
        spec = VdbenchSpec(
            workload=VdbenchWorkload.DW,
            target_path=f"{helix_config.nfs_export}/vdbench_dw",
            data_size_gb=100,
            threads=8,
            warmup_secs=15,
            runtime_secs=60,
        )

        with allure.step("Run vdbench DW (60s + 15s warmup)"):
            result = vdbench_runner.run(spec=spec)

        allure.attach(
            f"Throughput: {result.throughput_mbs:.1f} MB/s\n"
            f"IOPS: {result.iops:,.0f}",
            name="vdbench_dw_results.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

        comparison = baseline_comparator.compare(
            "vdbench_dw_mbs", result.throughput_mbs, unit=" MB/s"
        )
        baseline_reporter.attach_to_allure([comparison], test_name="test_dw_throughput")
        baseline_reporter.print_summary([comparison])
        baseline_reporter.assert_no_regressions([comparison])

        assert result.throughput_mbs >= 400.0, (
            f"DW throughput {result.throughput_mbs:.1f} MB/s < 400 MB/s minimum"
        )
