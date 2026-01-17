from typing import List, Optional

from pydantic import BaseModel, Field

from airflow_evals.runner.config import EvaluationSummary


class RegressionThresholds(BaseModel):
    """Configuration for regression thresholds."""

    accuracy_drop_max: float = Field(
        0.05, description="Max allowed absolute drop in accuracy (e.g. 0.05 = 5%)"
    )
    latency_p95_increase_max: float = Field(
        0.20, description="Max allowed relative increase in p95 latency (e.g. 0.20 = 20%)"
    )


class RegressionReport(BaseModel):
    """Result of regression check."""

    is_regression: bool
    details: List[str] = Field(default_factory=list)
    baseline_run_id: Optional[str] = None
    curr_accuracy: float
    base_accuracy: float
    curr_latency: float
    base_latency: float


class RegressionDetector:
    """Detector for evaluation regressions."""

    def __init__(self, thresholds: RegressionThresholds = RegressionThresholds()):
        """Initialize detector with thresholds."""
        self.thresholds = thresholds

    def check_regression(
        self, current: EvaluationSummary, baseline: Optional[EvaluationSummary]
    ) -> RegressionReport:
        """Compare current run against baseline."""
        if not baseline:
            return RegressionReport(
                is_regression=False,
                details=["No baseline provided."],
                curr_accuracy=current.accuracy,
                base_accuracy=0.0,
                curr_latency=current.p95_latency_ms,
                base_latency=0.0,
            )

        details = []
        is_regression = False

        # Check Accuracy
        # e.g. Baseline 0.9, Current 0.8 -> Drop 0.1 > Threshold 0.05 -> Regression
        accuracy_drop = baseline.accuracy - current.accuracy
        if accuracy_drop > self.thresholds.accuracy_drop_max:
            is_regression = True
            details.append(
                f"Accuracy dropped by {accuracy_drop:.2%} "
                f"(Threshold: {self.thresholds.accuracy_drop_max:.2%})"
            )

        # Check Latency (P95)
        # e.g. Baseline 100ms, Current 150ms -> Increase 50ms (50%) > Threshold 20% -> Regression
        # Only check if baseline latency is significant to avoid noise
        if baseline.p95_latency_ms > 1.0:
            latency_increase_ratio = (
                current.p95_latency_ms - baseline.p95_latency_ms
            ) / baseline.p95_latency_ms
            if latency_increase_ratio > self.thresholds.latency_p95_increase_max:
                is_regression = True
                details.append(
                    f"P95 Latency increased by {latency_increase_ratio:.2%} "
                    f"(Threshold: {self.thresholds.latency_p95_increase_max:.2%})"
                )

        return RegressionReport(
            is_regression=is_regression,
            details=details,
            baseline_run_id=baseline.run_id,
            curr_accuracy=current.accuracy,
            base_accuracy=baseline.accuracy,
            curr_latency=current.p95_latency_ms,
            base_latency=baseline.p95_latency_ms,
        )
