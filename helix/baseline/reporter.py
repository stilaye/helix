"""
Baseline reporter — formats performance comparison results for Allure and terminal output.

Attaches structured JSON summary and human-readable text to Allure reports.
Regressions and warnings are highlighted in terminal output via Rich.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from helix.baseline.comparator import ComparisonResult, ComparisonOutcome

logger = logging.getLogger(__name__)


class BaselineReporter:
    """
    Build Allure attachments and terminal summaries from BaselineComparator results.

    Usage:
        results = comparator.compare_batch(measurements)
        reporter = BaselineReporter()
        reporter.attach_to_allure(results, test_name="test_fio_random_read")
        reporter.print_summary(results)
        reporter.assert_no_regressions(results)
    """

    def attach_to_allure(
        self,
        results: list["ComparisonResult"],
        test_name: str = "",
    ) -> None:
        """Attach performance comparison summary to current Allure test."""
        try:
            import allure
        except ImportError:
            logger.debug("allure-pytest not installed — skipping Allure attachment")
            return

        summary = self._build_summary(results, test_name)

        # Attach structured JSON for downstream processing
        allure.attach(
            json.dumps(summary, indent=2),
            name="performance_baseline_summary.json",
            attachment_type=allure.attachment_type.JSON,
        )

        # Attach human-readable text
        allure.attach(
            self._build_text_report(results),
            name="performance_baseline_report.txt",
            attachment_type=allure.attachment_type.TEXT,
        )

        # Create individual Allure steps for each metric
        for result in results:
            with allure.step(f"Baseline: {result.metric}"):
                allure.attach(
                    result.message,
                    name=f"{result.metric}_result.txt",
                    attachment_type=allure.attachment_type.TEXT,
                )

    def print_summary(self, results: list["ComparisonResult"]) -> None:
        """Print a formatted summary to terminal using Rich if available."""
        try:
            from rich.console import Console
            from rich.table import Table
            from rich import box

            console = Console()
            table = Table(
                title="Performance Baseline Comparison",
                box=box.ROUNDED,
                show_header=True,
                header_style="bold cyan",
            )
            table.add_column("Metric", style="white", no_wrap=True)
            table.add_column("Current", justify="right")
            table.add_column("Baseline Mean", justify="right")
            table.add_column("Delta %", justify="right")
            table.add_column("Outcome", justify="center")

            for r in results:
                delta_str = f"{r.delta_pct*100:+.1f}%" if r.delta_pct is not None else "—"
                mean_str = f"{r.baseline_mean:.1f}" if r.baseline_mean is not None else "—"

                outcome_style = {
                    "regression": "[bold red]REGRESSION[/]",
                    "warning": "[yellow]WARNING[/]",
                    "improvement": "[bold green]IMPROVEMENT[/]",
                    "pass": "[green]PASS[/]",
                    "no_baseline": "[dim]NO BASELINE[/]",
                }.get(r.outcome.value, r.outcome.value)

                delta_style = "red" if (r.delta_pct or 0) < -0.02 else (
                    "green" if (r.delta_pct or 0) > 0.02 else "white"
                )

                table.add_row(
                    r.metric,
                    f"{r.current:.1f}",
                    mean_str,
                    f"[{delta_style}]{delta_str}[/]",
                    outcome_style,
                )

            console.print(table)

        except ImportError:
            # Fallback to plain text if Rich not installed
            print("\n=== Performance Baseline Comparison ===")
            for r in results:
                delta_str = f"{r.delta_pct*100:+.1f}%" if r.delta_pct is not None else "n/a"
                print(f"  {r.outcome.value.upper():12s} {r.metric}: {r.current:.1f} (delta: {delta_str})")
            print()

    def assert_no_regressions(
        self,
        results: list["ComparisonResult"],
        fail_on_warning: bool = False,
    ) -> None:
        """
        Raise AssertionError if any regressions detected.

        Args:
            results: List from comparator.compare_batch().
            fail_on_warning: If True, warnings also cause failure (strict mode).
        """
        from helix.baseline.comparator import ComparisonOutcome

        regressions = [r for r in results if r.outcome == ComparisonOutcome.REGRESSION]
        warnings = [r for r in results if r.outcome == ComparisonOutcome.WARNING]

        if regressions:
            msgs = "\n".join(f"  • {r.message}" for r in regressions)
            raise AssertionError(f"Performance regressions detected:\n{msgs}")

        if fail_on_warning and warnings:
            msgs = "\n".join(f"  • {r.message}" for r in warnings)
            raise AssertionError(f"Performance warnings (strict mode):\n{msgs}")

    # ─── Private helpers ──────────────────────────────────────────────────────

    def _build_summary(
        self, results: list["ComparisonResult"], test_name: str
    ) -> dict:
        return {
            "test_name": test_name,
            "metrics": [
                {
                    "metric": r.metric,
                    "current": r.current,
                    "baseline_mean": r.baseline_mean,
                    "baseline_stddev": r.baseline_stddev,
                    "delta_pct": round(r.delta_pct * 100, 2) if r.delta_pct else None,
                    "outcome": r.outcome.value,
                    "message": r.message,
                }
                for r in results
            ],
            "regression_count": sum(1 for r in results if r.is_regression),
            "warning_count": sum(1 for r in results if r.is_warning),
        }

    def _build_text_report(self, results: list["ComparisonResult"]) -> str:
        lines = ["Performance Baseline Report", "=" * 40]
        for r in results:
            delta_str = f"{r.delta_pct*100:+.1f}%" if r.delta_pct is not None else "n/a"
            mean_str = f"{r.baseline_mean:.1f}" if r.baseline_mean is not None else "n/a"
            lines.append(
                f"[{r.outcome.value.upper():12s}] {r.metric}: "
                f"current={r.current:.1f}, mean={mean_str}, delta={delta_str}"
            )
        return "\n".join(lines)
