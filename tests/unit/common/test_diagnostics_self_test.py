"""Unit tests for diagnostics self-test helpers."""

from common.config.diagnostics_self_test import run_diagnostics_self_test


def test_run_diagnostics_self_test_returns_structured_health_report():
    """Self-test should exercise fake validation/execution paths with bounded shape."""
    report = run_diagnostics_self_test()

    assert report["status"] in {"ok", "degraded", "error"}
    assert report["validation"]["status"] in {"ok", "degraded", "error"}
    assert report["execution"]["status"] in {"ok", "degraded", "error"}
