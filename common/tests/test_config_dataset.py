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

    def test_explicit_pagila(self, monkeypatch):
        """Explicit DATASET_MODE=pagila should work."""
        monkeypatch.setenv("DATASET_MODE", "pagila")
        from common.config.dataset import get_dataset_mode

        assert get_dataset_mode() == "pagila"

    def test_case_insensitive(self, monkeypatch):
        """DATASET_MODE should be case-insensitive."""
        monkeypatch.setenv("DATASET_MODE", "SYNTHETIC")
        from common.config.dataset import get_dataset_mode

        assert get_dataset_mode() == "synthetic"

        monkeypatch.setenv("DATASET_MODE", "Pagila")
        assert get_dataset_mode() == "pagila"

    def test_invalid_mode_raises(self, monkeypatch):
        """Invalid DATASET_MODE should raise ValueError."""
        monkeypatch.setenv("DATASET_MODE", "invalid")
        from common.config.dataset import get_dataset_mode

        with pytest.raises(ValueError, match="Invalid DATASET_MODE"):
            get_dataset_mode()


class TestGetDefaultDbName:
    """Tests for get_default_db_name function."""

    def test_synthetic_mode_returns_synthetic(self, monkeypatch):
        """Synthetic mode should return 'synthetic' as db name."""
        monkeypatch.setenv("DATASET_MODE", "synthetic")
        from common.config.dataset import get_default_db_name

        assert get_default_db_name() == "synthetic"

    def test_pagila_mode_returns_pagila(self, monkeypatch):
        """Pagila mode should return 'pagila' as db name."""
        monkeypatch.setenv("DATASET_MODE", "pagila")
        from common.config.dataset import get_default_db_name

        assert get_default_db_name() == "pagila"

    def test_default_returns_synthetic(self, monkeypatch):
        """Default (no DATASET_MODE) should return 'synthetic'."""
        monkeypatch.delenv("DATASET_MODE", raising=False)
        from common.config.dataset import get_default_db_name

        assert get_default_db_name() == "synthetic"
