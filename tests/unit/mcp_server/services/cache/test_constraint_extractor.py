"""Unit tests for constraint extraction from natural language queries."""

import pytest

from mcp_server.services.cache.constraint_extractor import extract_constraints


@pytest.fixture
def mode_synthetic(monkeypatch):
    """Fixture to set DATASET_MODE to synthetic."""
    monkeypatch.setenv("DATASET_MODE", "synthetic")


@pytest.mark.usefixtures("mode_synthetic")
class TestExtractConstraintsSynthetic:
    """Tests for extract_constraints in Synthetic (default) mode."""

    def test_no_film_rating_leakage(self):
        """Verify that film ratings are NOT extracted in synthetic mode."""
        constraints = extract_constraints("Show top 10 PG-13 movies")
        assert constraints.rating is None
        # Confidence should be default (0.5) or determined by other matches
        # not penalized for missing rating. If matches nothing, valid match with default.
        assert constraints.confidence == 0.5

    def test_no_film_entity_leakage(self):
        """Verify that film entities are NOT extracted in synthetic mode."""
        constraints = extract_constraints("Show top actors")
        assert constraints.entity is None

    def test_extract_financial_entity(self):
        """Test extraction of financial entities (currently disabled/empty)."""
        query = "Show top 10 merchants by transaction count"
        constraints = extract_constraints(query)
        # Patterns restored in Phase 2 fix, so entity should be merchant
        assert constraints.entity == "merchant"
        assert constraints.limit == 10

    def test_extract_limit_generic(self):
        """Test generic limit extraction works in synthetic mode."""
        constraints = extract_constraints("Top 5 items")
        assert constraints.limit == 5

    def test_confidence_without_rating(self):
        """Test confidence calculation in synthetic mode."""
        # In synthetic mode, "no rating" is normal,
        # so confidence shouldn't be penalized purely for that.
        constraints = extract_constraints("Show top 10 items")
        assert constraints.confidence == 0.5
