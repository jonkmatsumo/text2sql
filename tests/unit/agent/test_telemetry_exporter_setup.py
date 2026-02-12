"""Tests for OTEL exporter setup behavior in agent telemetry."""

from unittest.mock import MagicMock, patch

import pytest

import agent.telemetry as telemetry_mod


@pytest.fixture(autouse=True)
def _reset_otel_setup_flag():
    original = telemetry_mod._otel_initialized
    telemetry_mod._otel_initialized = False
    yield
    telemetry_mod._otel_initialized = original


def test_setup_otel_sdk_uses_in_memory_exporter_in_test_mode(monkeypatch):
    """Pytest mode should use in-memory exporter by default."""
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "tests/unit/agent/test_x.py::test")
    monkeypatch.setenv("OTEL_WORKER_ENABLED", "true")
    monkeypatch.setenv("OTEL_WORKER_REQUIRED", "false")
    monkeypatch.delenv("OTEL_DISABLE_EXPORTER", raising=False)
    monkeypatch.delenv("OTEL_TEST_EXPORTER", raising=False)

    fake_provider = MagicMock()

    with (
        patch.object(telemetry_mod, "TracerProvider", return_value=fake_provider),
        patch.object(telemetry_mod.trace, "set_tracer_provider"),
    ):
        telemetry_mod._setup_otel_sdk()

    assert fake_provider.add_span_processor.call_count == 1


def test_setup_otel_sdk_required_mode_raises_on_exporter_failure(monkeypatch):
    """OTEL_WORKER_REQUIRED=true should fail fast when exporter initialization fails."""
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "tests/unit/agent/test_x.py::test")
    monkeypatch.setenv("OTEL_WORKER_ENABLED", "true")
    monkeypatch.setenv("OTEL_WORKER_REQUIRED", "true")
    monkeypatch.setenv("OTEL_TEST_EXPORTER", "in_memory")
    monkeypatch.delenv("OTEL_DISABLE_EXPORTER", raising=False)

    fake_provider = MagicMock()

    with (
        patch.object(telemetry_mod, "TracerProvider", return_value=fake_provider),
        patch.object(telemetry_mod.trace, "set_tracer_provider"),
        patch(
            "common.observability.in_memory_exporter.get_or_create_span_exporter",
            side_effect=RuntimeError("exporter boom"),
        ),
    ):
        with pytest.raises(RuntimeError, match="OTEL_WORKER_REQUIRED=true"):
            telemetry_mod._setup_otel_sdk()
