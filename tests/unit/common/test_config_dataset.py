"""Tests for dataset mode configuration."""

import pytest


class TestGetDatasetMode:
    """Tests for get_dataset_mode function."""

    def test_default_is_synthetic(self, monkeypatch):
        """Default dataset mode should be 'synthetic'."""
        monkeypatch.delenv("DATASET_MODE", raising=False)
        from common.config.dataset import get_dataset_mode

        assert get_dataset_mode() == "synthetic"

    def test_explicit_synthetic(self, monkeypatch):
        """Explicit DATASET_MODE=synthetic should work."""
        monkeypatch.setenv("DATASET_MODE", "synthetic")
        from common.config.dataset import get_dataset_mode

        assert get_dataset_mode() == "synthetic"

    def test_case_insensitive(self, monkeypatch):
        """DATASET_MODE should be case-insensitive."""
        monkeypatch.setenv("DATASET_MODE", "SYNTHETIC")
        from common.config.dataset import get_dataset_mode

        assert get_dataset_mode() == "synthetic"

    def test_invalid_mode_raises(self, monkeypatch):
        """Invalid DATASET_MODE should raise ValueError."""
        monkeypatch.setenv("DATASET_MODE", "invalid")
        from common.config.dataset import get_dataset_mode

        with pytest.raises(ValueError, match="Invalid DATASET_MODE"):
            get_dataset_mode()


class TestGetDefaultDbName:
    """Tests for get_default_db_name function."""

    def test_synthetic_mode_returns_synthetic(self, monkeypatch):
        """Synthetic mode should return 'query_target' as db name."""
        monkeypatch.setenv("DATASET_MODE", "synthetic")
        from common.config.dataset import get_default_db_name

        assert get_default_db_name() == "query_target"

    def test_default_returns_synthetic(self, monkeypatch):
        """Default (no DATASET_MODE) should return 'query_target'."""
        monkeypatch.delenv("DATASET_MODE", raising=False)
        from common.config.dataset import get_default_db_name

        assert get_default_db_name() == "query_target"
