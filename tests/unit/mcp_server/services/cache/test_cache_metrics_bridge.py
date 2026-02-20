"""Tests for semantic cache in-memory metrics OTEL bridge."""

from unittest.mock import patch

from mcp_server.services.cache.metrics import CacheMetrics


def test_cache_metrics_bridge_emits_core_counters_and_histograms():
    """Cache metric mutations should emit corresponding OTEL counters/histograms."""
    metrics = CacheMetrics()

    with (
        patch("mcp_server.services.cache.metrics.mcp_metrics.add_counter") as mock_add_counter,
        patch("mcp_server.services.cache.metrics.mcp_metrics.record_histogram") as mock_histogram,
    ):
        metrics.record_guardrail_failure({"reason": "mismatch"})
        metrics.record_tombstone("cache-1", "invalid")
        metrics.record_cache_hit(0.91)
        metrics.record_cache_miss()
        metrics.record_extraction_failure("query", "parse error")
        metrics.record_semantic_ambiguity(0.82, 0.79)

    counter_names = [call.args[0] for call in mock_add_counter.call_args_list]
    assert "mcp.cache.guardrail_failures_total" in counter_names
    assert "mcp.cache.tombstones_total" in counter_names
    assert "mcp.cache.hits_total" in counter_names
    assert "mcp.cache.misses_total" in counter_names
    assert "mcp.cache.extraction_failures_total" in counter_names
    assert "mcp.cache.semantic_ambiguity_total" in counter_names
    assert mock_histogram.call_count >= 2
