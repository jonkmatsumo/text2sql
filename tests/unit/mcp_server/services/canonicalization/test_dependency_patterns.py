"""Tests for dataset-mode aware dependency patterns."""


class TestGetRatingPatterns:
    """Tests for get_rating_patterns function."""

    def test_synthetic_mode_returns_empty(self, monkeypatch):
        """Synthetic mode should return empty rating patterns (no film ratings)."""
        monkeypatch.setenv("DATASET_MODE", "synthetic")
        from mcp_server.services.canonicalization.dependency_patterns import get_rating_patterns

        patterns = get_rating_patterns()
        assert patterns == []

    def test_pagila_mode_returns_rating_patterns(self, monkeypatch):
        """Pagila mode should return film rating patterns."""
        monkeypatch.setenv("DATASET_MODE", "pagila")
        from mcp_server.services.canonicalization.dependency_patterns import (
            RATING_PATTERNS,
            get_rating_patterns,
        )

        patterns = get_rating_patterns()
        assert patterns == RATING_PATTERNS
        assert len(patterns) > 0

    def test_default_returns_empty(self, monkeypatch):
        """Default mode (no DATASET_MODE) should return empty patterns."""
        monkeypatch.delenv("DATASET_MODE", raising=False)
        from mcp_server.services.canonicalization.dependency_patterns import get_rating_patterns

        patterns = get_rating_patterns()
        assert patterns == []


class TestGetEntityPatterns:
    """Tests for get_entity_patterns function."""

    def test_synthetic_mode_returns_financial_only(self, monkeypatch):
        """Synthetic mode should only return financial entity patterns."""
        monkeypatch.setenv("DATASET_MODE", "synthetic")
        from mcp_server.services.canonicalization.dependency_patterns import (
            ENTITY_PATTERN_FINANCIAL,
            get_entity_patterns,
        )

        patterns = get_entity_patterns()
        assert len(patterns) == 1
        assert patterns[0] == ENTITY_PATTERN_FINANCIAL

    def test_pagila_mode_returns_all_entities(self, monkeypatch):
        """Pagila mode should return film, actor, and financial patterns."""
        monkeypatch.setenv("DATASET_MODE", "pagila")
        from mcp_server.services.canonicalization.dependency_patterns import get_entity_patterns

        patterns = get_entity_patterns()
        assert len(patterns) == 3


class TestGetAllPatterns:
    """Tests for get_all_patterns function."""

    def test_returns_dict_with_required_keys(self, monkeypatch):
        """get_all_patterns should return dict with rating, limit, entity keys."""
        monkeypatch.setenv("DATASET_MODE", "synthetic")
        from mcp_server.services.canonicalization.dependency_patterns import get_all_patterns

        result = get_all_patterns()
        assert "rating" in result
        assert "limit" in result
        assert "entity" in result

    def test_limit_patterns_always_included(self, monkeypatch):
        """LIMIT_PATTERNS should be included regardless of mode (domain-agnostic)."""
        from mcp_server.services.canonicalization.dependency_patterns import (
            LIMIT_PATTERNS,
            get_all_patterns,
        )

        monkeypatch.setenv("DATASET_MODE", "synthetic")
        result = get_all_patterns()
        assert result["limit"] == LIMIT_PATTERNS

        monkeypatch.setenv("DATASET_MODE", "pagila")
        result = get_all_patterns()
        assert result["limit"] == LIMIT_PATTERNS
