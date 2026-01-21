from unittest.mock import MagicMock, patch

from otel_worker.metrics.regression import calculate_percentile, compute_regressions


def test_calculate_percentile():
    """Test percentile calculation."""
    values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    # P50 of 1..10 is 5.5
    assert calculate_percentile(values, 50) == 5.5
    # P90
    assert calculate_percentile(values, 90) == 9.1
    # P99
    assert calculate_percentile(values, 99) == 9.91

    assert calculate_percentile([], 50) == 0.0


@patch("otel_worker.metrics.regression.datetime")
def test_compute_regressions_no_data(mock_dt):
    """Test regression computation with no data."""
    mock_dt.now.return_value = MagicMock()
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    # Return None for stats
    mock_conn.execute.return_value.fetchone.return_value = None

    count = compute_regressions(mock_engine, min_samples=5)
    assert count == 0


@patch("otel_worker.metrics.regression.datetime")
def test_compute_regressions_regression_detected(mock_dt):
    """Test that regressions are correctly detected and persisted."""
    mock_dt.now.return_value = MagicMock()
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    # Mock stats return
    # First call: candidate (bad latency)
    # Second call: baseline (good latency)

    # Stats object structure matching the query
    from collections import namedtuple

    Stats = namedtuple(
        "Stats",
        ["count", "lat_p50", "lat_p90", "lat_p99", "error_rate", "tokens_avg", "tokens_p95"],
    )

    cand_stats = Stats(20, 500.0, 1000.0, 2000.0, 0.0, 100, 150)
    base_stats = Stats(20, 100.0, 200.0, 300.0, 0.0, 100, 150)

    mock_conn.execute.return_value.fetchone.side_effect = [cand_stats, base_stats]

    # Mock top trace IDs query
    mock_conn.execute.return_value.__iter__.return_value = ["t1", "t2"]

    count = compute_regressions(
        mock_engine, min_samples=10, threshold_pct_latency=20.0, threshold_abs_latency_ms=50.0
    )

    # Should detect 3 latency regressions (p50, p90, p99)
    assert count == 3

    # Verify insert call
    assert (
        mock_conn.execute.call_count >= 5
    )  # 2 stats + 3 regressions inserts + potentially top trace ids queries
