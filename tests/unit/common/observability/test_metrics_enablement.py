"""Tests for OTEL-aware metrics enablement defaults."""

from unittest.mock import MagicMock, patch

from common.observability.metrics import OptionalMetrics, is_metrics_enabled


def test_metrics_enabled_when_exporter_configured_without_explicit_flag(monkeypatch):
    """Exporter configuration should enable metrics by default."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    monkeypatch.delenv("AGENT_OBSERVABILITY_METRICS_ENABLED", raising=False)

    assert is_metrics_enabled("AGENT_OBSERVABILITY_METRICS_ENABLED") is True


def test_metrics_explicit_false_overrides_exporter_default(monkeypatch):
    """Explicit false must disable metrics even when exporter is configured."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    monkeypatch.setenv("AGENT_OBSERVABILITY_METRICS_ENABLED", "false")

    assert is_metrics_enabled("AGENT_OBSERVABILITY_METRICS_ENABLED") is False


def test_metrics_explicit_true_overrides_exporter_default(monkeypatch):
    """Explicit true should keep metrics enabled when exporter is configured."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    monkeypatch.setenv("AGENT_OBSERVABILITY_METRICS_ENABLED", "true")

    assert is_metrics_enabled("AGENT_OBSERVABILITY_METRICS_ENABLED") is True


def test_optional_metrics_emits_counter_when_exporter_configured(monkeypatch):
    """Emit counters when exporter-driven defaults enable metrics."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    monkeypatch.delenv("AGENT_OBSERVABILITY_METRICS_ENABLED", raising=False)

    metric = OptionalMetrics(
        meter_name="test-agent",
        enabled_env_var="AGENT_OBSERVABILITY_METRICS_ENABLED",
    )
    fake_meter = MagicMock()
    fake_counter = MagicMock()
    fake_meter.create_counter.return_value = fake_counter

    with patch("common.observability.metrics.metrics.get_meter", return_value=fake_meter):
        metric.add_counter("agent.test.counter", value=3)

    fake_counter.add.assert_called_once()


def test_optional_metrics_skips_counter_when_explicitly_disabled(monkeypatch):
    """Explicit disable should skip metric emission and avoid meter initialization."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    monkeypatch.setenv("AGENT_OBSERVABILITY_METRICS_ENABLED", "false")

    metric = OptionalMetrics(
        meter_name="test-agent",
        enabled_env_var="AGENT_OBSERVABILITY_METRICS_ENABLED",
    )

    with patch("common.observability.metrics.metrics.get_meter") as mock_get_meter:
        metric.add_counter("agent.test.counter", value=1)

    mock_get_meter.assert_not_called()


def test_optional_metrics_registers_histogram_once_per_name(monkeypatch):
    """Histogram instruments should be cached and reused across emissions."""
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
    monkeypatch.delenv("AGENT_OBSERVABILITY_METRICS_ENABLED", raising=False)

    metric = OptionalMetrics(
        meter_name="test-agent",
        enabled_env_var="AGENT_OBSERVABILITY_METRICS_ENABLED",
    )
    fake_meter = MagicMock()
    fake_histogram = MagicMock()
    fake_meter.create_histogram.return_value = fake_histogram

    with patch("common.observability.metrics.metrics.get_meter", return_value=fake_meter):
        metric.record_histogram("agent.test.latency_ms", value=1.0)
        metric.record_histogram("agent.test.latency_ms", value=2.0)

    fake_meter.create_histogram.assert_called_once_with(
        name="agent.test.latency_ms",
        description="",
        unit="1",
    )
    assert fake_histogram.record.call_count == 2
