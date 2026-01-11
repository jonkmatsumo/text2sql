"""Unit tests for constraint extraction from natural language queries."""

from mcp_server.services.cache.constraint_extractor import extract_constraints, normalize_rating


class TestExtractConstraints:
    """Tests for extract_constraints function."""

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

    def test_extract_limit_top_10(self):
        """Test extraction of 'top 10' limit."""
        constraints = extract_constraints("Show top 10 actors")
        assert constraints.limit == 10

    def test_extract_limit_top_5(self):
        """Test extraction of 'top 5' limit."""
        constraints = extract_constraints("Top 5 films by revenue")
        assert constraints.limit == 5

    def test_extract_ties_including(self):
        """Test extraction of 'including ties'."""
        constraints = extract_constraints("Top 10 actors including ties")
        assert constraints.include_ties is True

    def test_extract_ties_with(self):
        """Test extraction of 'with ties'."""
        constraints = extract_constraints("Top 10 actors with ties")
        assert constraints.include_ties is True

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

    def test_extract_metric_count_distinct(self):
        """Test extraction of count distinct metric."""
        constraints = extract_constraints("Count of distinct films per actor")
        assert constraints.metric == "count_distinct"

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

    def test_confidence_without_rating(self):
        """Test that confidence is low when no rating found."""
        constraints = extract_constraints("Show all films")
        assert constraints.confidence < 0.5

    def test_priority_pg13_over_pg(self):
        """Test that PG-13 is matched before PG."""
        constraints = extract_constraints("Show PG-13 rated films")
        assert constraints.rating == "PG-13"


class TestNormalizeRating:
    """Tests for normalize_rating function."""

    def test_normalize_lowercase(self):
        """Test normalization of lowercase rating."""
        assert normalize_rating("pg") == "PG"

    def test_normalize_mixed_case(self):
        """Test normalization of mixed case rating."""
        assert normalize_rating("Pg-13") == "PG-13"

    def test_normalize_with_space(self):
        """Test normalization of rating with space."""
        assert normalize_rating("pg 13") == "PG-13"

    def test_normalize_nc17(self):
        """Test normalization of NC-17 variants."""
        assert normalize_rating("nc17") == "NC-17"
        assert normalize_rating("NC-17") == "NC-17"

    def test_normalize_invalid(self):
        """Test normalization of invalid rating."""
        assert normalize_rating("XX") is None

    def test_normalize_empty(self):
        """Test normalization of empty string."""
        assert normalize_rating("") is None

    def test_normalize_none(self):
        """Test normalization of None."""
        assert normalize_rating(None) is None
