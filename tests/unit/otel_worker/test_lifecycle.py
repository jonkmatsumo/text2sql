"""Lifecycle tests for OTEL worker startup and degraded behavior."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from otel_worker import app as worker_app


def _patch_worker_components():
    """Patch background component start/stop hooks for deterministic lifecycle tests."""
    return patch.multiple(
        worker_app,
        init_minio=MagicMock(),
        safe_queue=MagicMock(start=MagicMock(), stop=MagicMock(), dropped_items=0),
        monitor=MagicMock(start=AsyncMock(), stop=AsyncMock(), check_admissibility=MagicMock()),
        coordinator=MagicMock(start=AsyncMock(), stop=AsyncMock()),
        aggregation_coordinator=MagicMock(start=AsyncMock(), stop=AsyncMock()),
        regression_coordinator=MagicMock(start=AsyncMock(), stop=AsyncMock()),
        reconciliation_coordinator=MagicMock(start=AsyncMock(), stop=AsyncMock()),
    )


def test_worker_disabled_skips_startup_components(monkeypatch):
    """OTEL_WORKER_ENABLED=false should skip startup and report disabled health status."""
    monkeypatch.setattr(worker_app.settings, "OTEL_WORKER_ENABLED", False)
    monkeypatch.setattr(worker_app.settings, "OTEL_WORKER_REQUIRED", False)

    with patch.object(worker_app, "init_db") as mock_init_db:
        with TestClient(worker_app.app) as client:
            health = client.get("/healthz")

    assert mock_init_db.call_count == 0
    assert health.status_code == 200
    payload = health.json()
    assert payload["status"] == "disabled"
    assert payload["enabled"] is False
    assert payload["ingestion_available"] is False


def test_worker_non_required_degrades_when_startup_fails(monkeypatch):
    """Startup failures should degrade gracefully when worker is not required."""
    monkeypatch.setattr(worker_app.settings, "OTEL_WORKER_ENABLED", True)
    monkeypatch.setattr(worker_app.settings, "OTEL_WORKER_REQUIRED", False)

    with _patch_worker_components():
        with patch.object(worker_app, "init_db", side_effect=RuntimeError("db unavailable")):
            with TestClient(worker_app.app) as client:
                health = client.get("/healthz")
                ingest = client.post(
                    "/v1/traces",
                    content=b"{}",
                    headers={"content-type": "application/json"},
                )

    assert health.status_code == 200
    health_payload = health.json()
    assert health_payload["status"] == "degraded"
    assert health_payload["ingestion_available"] is False
    assert health_payload["startup_errors"]
    assert ingest.status_code == 202


def test_worker_required_fails_fast_on_startup_error(monkeypatch):
    """Startup failures must fail fast when OTEL_WORKER_REQUIRED=true."""
    monkeypatch.setattr(worker_app.settings, "OTEL_WORKER_ENABLED", True)
    monkeypatch.setattr(worker_app.settings, "OTEL_WORKER_REQUIRED", True)

    with _patch_worker_components():
        with patch.object(worker_app, "init_db", side_effect=RuntimeError("db unavailable")):
            with pytest.raises(
                RuntimeError, match="OTEL worker startup failed at 'storage.init_db'"
            ):
                with TestClient(worker_app.app):
                    pass


def test_worker_shutdown_flushes_tracer_provider(monkeypatch):
    """Worker shutdown should flush and shutdown tracer provider best-effort."""
    monkeypatch.setattr(worker_app.settings, "OTEL_WORKER_ENABLED", True)
    monkeypatch.setattr(worker_app.settings, "OTEL_WORKER_REQUIRED", False)

    fake_provider = MagicMock(force_flush=MagicMock(), shutdown=MagicMock())
    with _patch_worker_components():
        with (
            patch.object(worker_app, "init_db"),
            patch.object(worker_app.trace, "get_tracer_provider", return_value=fake_provider),
        ):
            with TestClient(worker_app.app):
                pass

    assert fake_provider.force_flush.call_count >= 1
    assert fake_provider.shutdown.call_count >= 1
