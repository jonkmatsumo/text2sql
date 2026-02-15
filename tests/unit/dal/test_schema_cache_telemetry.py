"""Unit tests for SchemaCache telemetry."""

from unittest.mock import MagicMock, patch

import pytest

from dal.schema_cache import SchemaCache, SchemaCacheBackend


@pytest.fixture
def mock_backend():
    """Mock backend that respects the protocol."""
    backend = MagicMock(spec=SchemaCacheBackend)
    backend.clear.return_value = 5  # Arbitrary count
    return backend


@pytest.fixture
def mock_tracer():
    """Mock OTEL tracer."""
    tracer = MagicMock()
    span = MagicMock()
    tracer.start_as_current_span.return_value.__enter__.return_value = span
    return tracer


def test_invalidate_telemetry_global(mock_backend, mock_tracer):
    """Test global invalidation telemetry."""
    with patch("dal.schema_cache.trace.get_tracer", return_value=mock_tracer):
        cache = SchemaCache(backend=mock_backend)
        cache.invalidate()

        # Verify backend call
        mock_backend.clear.assert_called_once_with(provider=None, schema=None, table=None)

        # Verify telemetry
        mock_tracer.start_as_current_span.assert_called_once_with("schema.cache.invalidate")
        span = mock_tracer.start_as_current_span.return_value.__enter__.return_value

        # Check standard attributes
        span.set_attribute.assert_any_call("schema.cache.scope", "global")
        span.set_attribute.assert_any_call("schema.cache.entries_cleared", 5)


def test_invalidate_telemetry_provider(mock_backend, mock_tracer):
    """Test provider-scoped invalidation telemetry."""
    with patch("dal.schema_cache.trace.get_tracer", return_value=mock_tracer):
        cache = SchemaCache(backend=mock_backend)
        cache.invalidate(provider="postgres")

        mock_backend.clear.assert_called_once_with(provider="postgres", schema=None, table=None)

        span = mock_tracer.start_as_current_span.return_value.__enter__.return_value
        span.set_attribute.assert_any_call("schema.cache.scope", "provider")
        span.set_attribute.assert_any_call("schema.cache.provider", "postgres")
        span.set_attribute.assert_any_call("schema.cache.entries_cleared", 5)


def test_invalidate_telemetry_schema(mock_backend, mock_tracer):
    """Test schema-scoped invalidation telemetry."""
    with patch("dal.schema_cache.trace.get_tracer", return_value=mock_tracer):
        cache = SchemaCache(backend=mock_backend)
        cache.invalidate(provider="postgres", schema="public")

        mock_backend.clear.assert_called_once_with(provider="postgres", schema="public", table=None)

        span = mock_tracer.start_as_current_span.return_value.__enter__.return_value
        span.set_attribute.assert_any_call("schema.cache.scope", "schema")
        span.set_attribute.assert_any_call("schema.cache.provider", "postgres")
        span.set_attribute.assert_any_call("schema.cache.schema_name", "public")
        span.set_attribute.assert_any_call("schema.cache.entries_cleared", 5)


def test_invalidate_telemetry_table(mock_backend, mock_tracer):
    """Test table-scoped invalidation telemetry."""
    with patch("dal.schema_cache.trace.get_tracer", return_value=mock_tracer):
        cache = SchemaCache(backend=mock_backend)
        cache.invalidate(provider="postgres", schema="public", table="users")

        mock_backend.clear.assert_called_once_with(
            provider="postgres", schema="public", table="users"
        )

        span = mock_tracer.start_as_current_span.return_value.__enter__.return_value
        span.set_attribute.assert_any_call("schema.cache.scope", "table")
        span.set_attribute.assert_any_call("schema.cache.provider", "postgres")
        span.set_attribute.assert_any_call("schema.cache.schema_name", "public")
        span.set_attribute.assert_any_call("schema.cache.table_name", "users")
        span.set_attribute.assert_any_call("schema.cache.entries_cleared", 5)
