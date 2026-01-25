"""Tests for observability deep-link utilities."""

import os
from unittest import mock


class TestObservabilityLinks:
    """Test URL builder functions."""

    def test_grafana_trace_detail_url_default(self):
        """Test Grafana URL with default base."""
        from ui.service.observability_links import grafana_trace_detail_url

        url = grafana_trace_detail_url("abc123")
        assert "localhost:3001" in url
        assert "d/text2sql-trace-detail" in url
        assert "var-trace_id=abc123" in url

    def test_otel_trace_url_default(self):
        """Test OTEL trace URL with default base."""
        from ui.service.observability_links import otel_trace_url

        url = otel_trace_url("abc123")
        assert "localhost:4320" in url
        assert "/api/v1/traces/abc123" in url

    def test_otel_spans_url_default(self):
        """Test OTEL spans URL with default base."""
        from ui.service.observability_links import otel_spans_url

        url = otel_spans_url("abc123")
        assert "localhost:4320" in url
        assert "/api/v1/traces/abc123/spans" in url
        assert "include=attributes" in url

    def test_otel_raw_url_default(self):
        """Test OTEL raw URL with default base."""
        from ui.service.observability_links import otel_raw_url

        url = otel_raw_url("abc123")
        assert "localhost:4320" in url
        assert "/api/v1/traces/abc123/raw" in url

    def test_grafana_url_with_env_override(self):
        """Test Grafana URL respects GRAFANA_BASE_URL env var."""
        with mock.patch.dict(os.environ, {"GRAFANA_BASE_URL": "https://grafana.example.com"}):
            # Force reimport to pick up new env var
            import importlib

            from ui.service import observability_links

            importlib.reload(observability_links)

            url = observability_links.grafana_trace_detail_url("xyz789")
            assert "grafana.example.com" in url
            assert "var-trace_id=xyz789" in url

            # Restore default
            importlib.reload(observability_links)

    def test_otel_url_with_env_override(self):
        """Test OTEL URL respects OTEL_WORKER_BASE_URL env var."""
        with mock.patch.dict(os.environ, {"OTEL_WORKER_BASE_URL": "https://otel.example.com"}):
            import importlib

            from ui.service import observability_links

            importlib.reload(observability_links)

            url = observability_links.otel_trace_url("xyz789")
            assert "otel.example.com" in url
            assert "/api/v1/traces/xyz789" in url

            # Restore default
            importlib.reload(observability_links)

    def test_module_imports_successfully(self):
        """Verify module can be imported without errors."""
        from ui.service import observability_links

        assert hasattr(observability_links, "grafana_trace_detail_url")
        assert hasattr(observability_links, "otel_trace_url")
        assert hasattr(observability_links, "otel_spans_url")
        assert hasattr(observability_links, "otel_raw_url")
