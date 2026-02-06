"""Tests for dataset-mode aware dependency patterns."""


class TestGetRatingPatterns:
    """Tests for get_rating_patterns function."""

    def test_synthetic_mode_returns_empty(self, monkeypatch):
        """Synthetic mode should return empty rating patterns (no film ratings)."""
        monkeypatch.setenv("DATASET_MODE", "synthetic")
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
