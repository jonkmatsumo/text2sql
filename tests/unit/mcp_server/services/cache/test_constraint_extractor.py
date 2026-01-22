"""Unit tests for constraint extraction from natural language queries."""

import pytest

from mcp_server.services.cache.constraint_extractor import extract_constraints


@pytest.fixture
def mode_pagila(monkeypatch):
    """Fixture to set DATASET_MODE to pagila."""
    monkeypatch.setenv("DATASET_MODE", "pagila")


@pytest.fixture
def mode_synthetic(monkeypatch):
    """Fixture to set DATASET_MODE to synthetic."""
    monkeypatch.setenv("DATASET_MODE", "synthetic")


@pytest.mark.pagila
@pytest.mark.usefixtures("mode_pagila")
class TestExtractConstraintsPagila:
    """Tests for extract_constraints in Pagila (legacy) mode."""

    @pytest.fixture(autouse=True)
    def skip_if_not_pagila(self, dataset_mode):
        """Skip these tests if we are not explicitly running for Pagila."""
        # Even with mode_pagila fixture forcing the env var for the TEST,
        # we want to skip this whole class if the GLOBAL suite run is synthetic-default
        # and user didn't request pagila tests.
        import os

        # If RUN_PAGILA_TESTS is set, we run.
        if os.getenv("RUN_PAGILA_TESTS", "0") == "1":
            return

        # If the *original* environment (before monkeypatch) was pagila, we run.
        # But here checking the current env might match monkeypatch.
        # So we really rely on the marker skipping we implemented in conftest.
        # But since mcp-server conftest might lack the hook, we do an explicit check.
        # We check if the INTENT was to run pagila.
        # If we are just running 'pytest', dataset_mode fixture (session) says 'synthetic'.
        if dataset_mode != "pagila":
            pytest.skip(
                "Skipping Pagila tests in synthetic mode "
                "(set DATASET_MODE=pagila or RUN_PAGILA_TESTS=1)"
            )

    def test_extract_rating_g(self):
        """Test extraction of G rating."""
        constraints = extract_constraints("Top 10 actors in G films")
        assert constraints.rating == "G"

    def test_extract_rating_pg(self):
        """Test extraction of PG rating."""
        constraints = extract_constraints("Top 10 actors in PG rated movies")
        assert constraints.rating == "PG"

    def test_extract_rating_pg13(self):
        """Test extraction of PG-13 rating (hyphenated)."""
        constraints = extract_constraints("Show me PG-13 films")
        assert constraints.rating == "PG-13"

    def test_extract_rating_pg13_space(self):
        """Test extraction of PG 13 rating (with space)."""
        constraints = extract_constraints("Show me PG 13 movies")
        assert constraints.rating == "PG-13"

    def test_extract_rating_r(self):
        """Test extraction of R rating."""
        constraints = extract_constraints("Top 10 actors in R rated films")
        assert constraints.rating == "R"

    def test_extract_rating_nc17(self):
        """Test extraction of NC-17 rating."""
        constraints = extract_constraints("Show NC-17 content")
        assert constraints.rating == "NC-17"

    def test_extract_rating_nc17_no_hyphen(self):
        """Test extraction of NC17 rating (no hyphen)."""
        constraints = extract_constraints("Show NC17 movies")
        assert constraints.rating == "NC-17"

    def test_extract_entity_actor(self):
        """Test extraction of actor entity."""
        constraints = extract_constraints("Show top actors")
        assert constraints.entity == "actor"

    def test_extract_entity_film(self):
        """Test extraction of film entity."""
        constraints = extract_constraints("Show all films")
        assert constraints.entity == "film"

    def test_extract_entity_movie_as_film(self):
        """Test that 'movie' is normalized to 'film'."""
        constraints = extract_constraints("Show all movies")
        assert constraints.entity == "film"

    def test_extract_combined(self):
        """Test combined extraction of multiple constraints."""
        query = "Top 10 actors including ties in PG films by distinct film count"
        constraints = extract_constraints(query)

        assert constraints.rating == "PG"
        assert constraints.limit == 10
        assert constraints.include_ties is True
        assert constraints.entity == "actor"

    def test_confidence_with_rating(self):
        """Test that confidence is high when rating is found."""
        constraints = extract_constraints("Show G rated films")
        assert constraints.confidence >= 0.8

    def test_priority_pg13_over_pg(self):
        """Test that PG-13 is matched before PG."""
        constraints = extract_constraints("Show PG-13 rated films")
        assert constraints.rating == "PG-13"


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
