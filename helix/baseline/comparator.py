"""
Baseline comparator — detects performance regressions against historical samples.

Two-tier detection:
  1. Hard threshold: delta_pct < -5%          → REGRESSION (test fails)
  2. Statistical:    value < mean - 2*stddev  → REGRESSION (outlier detection)
  3. Soft warning:   -5% < delta_pct < -2%    → WARNING (logged, test passes)
  4. Improvement:    delta_pct > +5%          → INFO (never fails)

Why two tiers?
  A single stored value baseline is fragile — one noisy CI run sets a bad baseline.
  Rolling 5-sample mean + stddev eliminates false positives from infrastructure noise
  while still catching real regressions.

Usage:
    comparator = BaselineComparator(store)
    result = comparator.compare("fio_rand_read_iops", current_value=48_000.0)
    if result.is_regression:
        pytest.fail(result.message)
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from helix.baseline.store import BaselineStore

logger = logging.getLogger(__name__)


class ComparisonOutcome(Enum):
    NO_BASELINE = "no_baseline"       # Not enough history — record and pass
    IMPROVEMENT = "improvement"       # Significantly better than baseline
    PASS = "pass"                     # Within acceptable range
    WARNING = "warning"               # Mild degradation (-2% to -5%)
    REGRESSION = "regression"         # Hard fail


@dataclass(frozen=True)
class ComparisonResult:
    metric: str
    current: float
    baseline_mean: float | None
    baseline_stddev: float | None
    delta_pct: float | None
    outcome: ComparisonOutcome
    message: str

    @property
    def is_regression(self) -> bool:
        return self.outcome == ComparisonOutcome.REGRESSION

    @property
    def is_warning(self) -> bool:
        return self.outcome == ComparisonOutcome.WARNING

    @property
    def has_baseline(self) -> bool:
        return self.outcome != ComparisonOutcome.NO_BASELINE


class BaselineComparator:
    """
    Compare a current measurement against stored historical samples.

    Args:
        store: BaselineStore instance (JSONStore or SQLiteStore).
        hard_regression_pct: Percentage drop that causes hard failure (default: 5%).
        warning_pct: Percentage drop that triggers a non-failing warning (default: 2%).
        stddev_multiplier: How many stddevs below mean triggers regression (default: 2.0).
        min_samples: Minimum samples needed before comparisons are meaningful (default: 3).
    """

    def __init__(
        self,
        store: "BaselineStore",
        hard_regression_pct: float = 5.0,
        warning_pct: float = 2.0,
        stddev_multiplier: float = 2.0,
        min_samples: int = 3,
    ) -> None:
        self._store = store
        self._hard_threshold = hard_regression_pct / 100.0
        self._warning_threshold = warning_pct / 100.0
        self._stddev_mult = stddev_multiplier
        self._min_samples = min_samples

    def compare(
        self,
        metric: str,
        current: float,
        unit: str = "",
        higher_is_better: bool = True,
    ) -> ComparisonResult:
        """
        Compare current measurement against historical baseline.

        Args:
            metric: Metric name (e.g., "fio_rand_read_iops").
            current: Current measured value.
            unit: Display unit for messages (e.g., "IOPS", "MB/s", "ms").
            higher_is_better: True for IOPS/throughput, False for latency.

        Returns:
            ComparisonResult with outcome and human-readable message.
        """
        samples = self._store.get_samples(metric, limit=5)

        # Always record the current value
        self._store.record(metric, current)

        if len(samples) < self._min_samples:
            msg = (
                f"{metric}: {current:.1f}{unit} — "
                f"no baseline yet ({len(samples)}/{self._min_samples} samples collected)"
            )
            logger.info(msg)
            return ComparisonResult(
                metric=metric,
                current=current,
                baseline_mean=None,
                baseline_stddev=None,
                delta_pct=None,
                outcome=ComparisonOutcome.NO_BASELINE,
                message=msg,
            )

        mean = sum(samples) / len(samples)
        variance = sum((s - mean) ** 2 for s in samples) / len(samples)
        stddev = math.sqrt(variance)

        if higher_is_better:
            delta_pct = (current - mean) / mean if mean != 0 else 0.0
        else:
            # For latency: current > mean is a regression
            delta_pct = (mean - current) / mean if mean != 0 else 0.0

        # Statistical outlier check
        if higher_is_better:
            is_statistical_regression = (stddev > 0) and (current < mean - self._stddev_mult * stddev)
        else:
            is_statistical_regression = (stddev > 0) and (current > mean + self._stddev_mult * stddev)

        # Determine outcome
        if delta_pct <= -self._hard_threshold or is_statistical_regression:
            outcome = ComparisonOutcome.REGRESSION
            msg = (
                f"REGRESSION: {metric} = {current:.1f}{unit} "
                f"(baseline mean={mean:.1f}, delta={delta_pct*100:+.1f}%"
                f"{', statistical outlier' if is_statistical_regression else ''})"
            )
            logger.error(msg)

        elif delta_pct <= -self._warning_threshold:
            outcome = ComparisonOutcome.WARNING
            msg = (
                f"WARNING: {metric} = {current:.1f}{unit} "
                f"(baseline mean={mean:.1f}, delta={delta_pct*100:+.1f}% — mild degradation)"
            )
            logger.warning(msg)

        elif delta_pct >= self._hard_threshold:
            outcome = ComparisonOutcome.IMPROVEMENT
            msg = (
                f"IMPROVEMENT: {metric} = {current:.1f}{unit} "
                f"(baseline mean={mean:.1f}, delta={delta_pct*100:+.1f}%)"
            )
            logger.info(msg)

        else:
            outcome = ComparisonOutcome.PASS
            msg = (
                f"PASS: {metric} = {current:.1f}{unit} "
                f"(baseline mean={mean:.1f}, delta={delta_pct*100:+.1f}%)"
            )
            logger.info(msg)

        return ComparisonResult(
            metric=metric,
            current=current,
            baseline_mean=mean,
            baseline_stddev=stddev,
            delta_pct=delta_pct,
            outcome=outcome,
            message=msg,
        )

    def compare_batch(
        self,
        measurements: dict[str, tuple[float, str, bool]],
    ) -> list[ComparisonResult]:
        """
        Compare multiple metrics at once.

        Args:
            measurements: {metric_name: (value, unit, higher_is_better)}

        Returns:
            List of ComparisonResult, regressions first.
        """
        results = []
        for metric, (value, unit, higher_is_better) in measurements.items():
            results.append(self.compare(metric, value, unit, higher_is_better))
        results.sort(key=lambda r: r.outcome.value, reverse=True)
        return results
