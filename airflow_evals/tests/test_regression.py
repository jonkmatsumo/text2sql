import pytest

from airflow_evals.runner.config import EvaluationConfig, EvaluationSummary
from airflow_evals.runner.regression import RegressionDetector, RegressionThresholds


@pytest.fixture
def mock_summary():
    """Create a mock summary factory."""

    def _create(accuracy, latency):
        return EvaluationSummary(
            run_id="test",
            config=EvaluationConfig(dataset_path="d", output_dir="o"),
            total_cases=100,
            successful_cases=int(100 * accuracy),
            failed_cases=int(100 * (1 - accuracy)),
            accuracy=accuracy,
            avg_latency_ms=latency,
            p95_latency_ms=latency,
        )

    return _create


def test_no_baseline(mock_summary):
    """Test regression check when no baseline is provided."""
    detector = RegressionDetector()
    current = mock_summary(0.9, 100)
    report = detector.check_regression(current, None)
    assert not report.is_regression
    assert "No baseline" in report.details[0]


def test_accuracy_regression(mock_summary):
    """Test detection of accuracy regression."""
    # Threshold is 0.05
    detector = RegressionDetector(RegressionThresholds(accuracy_drop_max=0.05))

    # Baseline 0.9, Current 0.8 -> Drop 0.1 (Regression)
    baseline = mock_summary(0.9, 100)
    current = mock_summary(0.8, 100)
    report = detector.check_regression(current, baseline)
    assert report.is_regression
    assert "Accuracy dropped" in report.details[0]

    # Baseline 0.9, Current 0.86 -> Drop 0.04 (No Regression)
    current_ok = mock_summary(0.86, 100)
    report_ok = detector.check_regression(current_ok, baseline)
    assert not report_ok.is_regression


def test_latency_regression(mock_summary):
    """Test detection of latency regression."""
    # Threshold is 0.20 (20%)
    detector = RegressionDetector(RegressionThresholds(latency_p95_increase_max=0.20))

    # Baseline 100, Current 130 -> Increase 30% (Regression)
    baseline = mock_summary(0.9, 100)
    current = mock_summary(0.9, 130)
    report = detector.check_regression(current, baseline)
    assert report.is_regression
    assert "Latency increased" in report.details[0]

    # Baseline 100, Current 110 -> Increase 10% (No Regression)
    current_ok = mock_summary(0.9, 110)
    report_ok = detector.check_regression(current_ok, baseline)
    assert not report_ok.is_regression
