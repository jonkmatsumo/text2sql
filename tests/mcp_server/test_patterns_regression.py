"""Regression test for patterns module import stability."""

import pytest


def test_ingestion_patterns_importable():
    """Verify that ingestion.patterns can be imported."""
    try:
        import ingestion.patterns
        import ingestion.patterns.auditing
        import ingestion.patterns.enum_detector
        import ingestion.patterns.generator
        import ingestion.patterns.validator

        # Access members to avoid F401 (unused import) if linters are pedantic inside try
        assert ingestion.patterns
        assert ingestion.patterns.auditing
        assert ingestion.patterns.enum_detector
        assert ingestion.patterns.generator
        assert ingestion.patterns.validator
    except ImportError as e:
        pytest.fail(f"Failed to import ingestion.patterns modules: {e}")


def test_mcp_server_legacy_path_fail():
    """Verify that the old path is indeed gone (architecture adherence)."""
    with pytest.raises(ImportError):
        # noqa: F401
        import mcp_server.services.patterns  # noqa: F401
