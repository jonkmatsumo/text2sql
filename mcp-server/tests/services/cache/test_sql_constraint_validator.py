"""Unit tests for SQL constraint validation."""

import pytest
from mcp_server.services.cache.constraint_extractor import QueryConstraints
from mcp_server.services.cache.sql_constraint_validator import (
    extract_limit_from_sql,
    extract_rating_from_sql,
    validate_sql_constraints,
)


class TestExtractRatingFromSql:
    """Tests for extract_rating_from_sql function."""

    def test_extract_simple_eq(self):
        """Test extraction from simple equality."""
        sql = "SELECT * FROM film WHERE rating = 'PG'"
        assert extract_rating_from_sql(sql) == "PG"

    def test_extract_with_alias(self):
        """Test extraction with table alias."""
        sql = "SELECT * FROM film f WHERE f.rating = 'G'"
        assert extract_rating_from_sql(sql) == "G"

    def test_extract_pg13(self):
        """Test extraction of PG-13."""
        sql = "SELECT * FROM film WHERE rating = 'PG-13'"
        assert extract_rating_from_sql(sql) == "PG-13"

    def test_extract_nc17(self):
        """Test extraction of NC-17."""
        sql = "SELECT * FROM film WHERE rating = 'NC-17'"
        assert extract_rating_from_sql(sql) == "NC-17"

    def test_extract_r_rating(self):
        """Test extraction of R rating."""
        sql = "SELECT * FROM film WHERE rating = 'R'"
        assert extract_rating_from_sql(sql) == "R"

    def test_no_rating_predicate(self):
        """Test when no rating predicate exists."""
        sql = "SELECT * FROM film WHERE title = 'Test'"
        assert extract_rating_from_sql(sql) is None

    def test_complex_query_with_rating(self):
        """Test extraction from complex query."""
        sql = """
        SELECT a.first_name, COUNT(DISTINCT f.film_id) as film_count
        FROM actor a
        JOIN film_actor fa ON a.actor_id = fa.actor_id
        JOIN film f ON fa.film_id = f.film_id
        WHERE f.rating = 'PG'
        GROUP BY a.actor_id, a.first_name
        ORDER BY film_count DESC
        LIMIT 10
        """
        assert extract_rating_from_sql(sql) == "PG"


class TestExtractLimitFromSql:
    """Tests for extract_limit_from_sql function."""

    def test_extract_limit_10(self):
        """Test extraction of LIMIT 10."""
        sql = "SELECT * FROM film LIMIT 10"
        assert extract_limit_from_sql(sql) == 10

    def test_extract_limit_5(self):
        """Test extraction of LIMIT 5."""
        sql = "SELECT * FROM film ORDER BY title LIMIT 5"
        assert extract_limit_from_sql(sql) == 5

    def test_no_limit(self):
        """Test when no LIMIT exists."""
        sql = "SELECT * FROM film"
        assert extract_limit_from_sql(sql) is None


class TestValidateSqlConstraints:
    """Tests for validate_sql_constraints function."""

    def test_valid_rating_match(self):
        """Test validation passes when rating matches."""
        sql = "SELECT * FROM film WHERE rating = 'PG'"
        constraints = QueryConstraints(rating="PG")

        result = validate_sql_constraints(sql, constraints)

        assert result.is_valid is True
        assert len(result.mismatches) == 0

    def test_invalid_rating_mismatch(self):
        """Test validation fails when rating doesn't match."""
        sql = "SELECT * FROM film WHERE rating = 'G'"
        constraints = QueryConstraints(rating="PG")

        result = validate_sql_constraints(sql, constraints)

        assert result.is_valid is False
        assert len(result.mismatches) == 1
        assert result.mismatches[0].constraint_type == "rating"
        assert result.mismatches[0].expected == "PG"
        assert result.mismatches[0].found == "G"

    def test_invalid_missing_rating(self):
        """Test validation fails when rating is missing from SQL."""
        sql = "SELECT * FROM film WHERE title = 'Test'"
        constraints = QueryConstraints(rating="PG")

        result = validate_sql_constraints(sql, constraints)

        assert result.is_valid is False
        assert len(result.mismatches) == 1
        assert result.mismatches[0].found is None

    def test_valid_no_constraints(self):
        """Test validation passes when no constraints specified."""
        sql = "SELECT * FROM film"
        constraints = QueryConstraints()

        result = validate_sql_constraints(sql, constraints)

        assert result.is_valid is True

    def test_valid_limit_match(self):
        """Test validation passes when limit matches."""
        sql = "SELECT * FROM film LIMIT 10"
        constraints = QueryConstraints(limit=10)

        result = validate_sql_constraints(sql, constraints)

        assert result.is_valid is True

    def test_invalid_limit_mismatch(self):
        """Test validation fails when limit doesn't match."""
        sql = "SELECT * FROM film LIMIT 5"
        constraints = QueryConstraints(limit=10, include_ties=False)

        result = validate_sql_constraints(sql, constraints)

        assert result.is_valid is False
        assert result.mismatches[0].constraint_type == "limit"

    def test_limit_mismatch_allowed_with_ties(self):
        """Test limit mismatch is allowed when include_ties is True."""
        sql = "SELECT * FROM film LIMIT 15"  # More than 10 due to ties
        constraints = QueryConstraints(limit=10, include_ties=True)

        result = validate_sql_constraints(sql, constraints)

        # With ties, we allow different limits
        assert result.is_valid is True

    def test_complex_query_validation(self):
        """Test validation of complex query."""
        sql = """
        SELECT a.first_name, COUNT(DISTINCT f.film_id) as film_count
        FROM actor a
        JOIN film_actor fa ON a.actor_id = fa.actor_id
        JOIN film f ON fa.film_id = f.film_id
        WHERE f.rating = 'R'
        GROUP BY a.actor_id, a.first_name
        ORDER BY film_count DESC
        LIMIT 10
        """
        constraints = QueryConstraints(rating="R", limit=10, entity="actor")

        result = validate_sql_constraints(sql, constraints)

        assert result.is_valid is True
        assert result.extracted_predicates.get("rating") == "R"
        assert result.extracted_predicates.get("limit") == 10


class TestRatingVariants:
    """Test various rating query patterns to prevent cache aliasing."""

    @pytest.mark.parametrize(
        "query,expected_rating",
        [
            ("Top 10 actors in G films", "G"),
            ("Top 10 actors in PG films", "PG"),
            ("Top 10 actors in PG-13 films", "PG-13"),
            ("Top 10 actors in R films", "R"),
            ("Top 10 actors in NC-17 films", "NC-17"),
        ],
    )
    def test_rating_extraction_parametrized(self, query, expected_rating):
        """Parametrized test for rating extraction from various queries."""
        from mcp_server.services.cache.constraint_extractor import extract_constraints

        constraints = extract_constraints(query)
        assert constraints.rating == expected_rating

    @pytest.mark.parametrize(
        "sql,expected_rating",
        [
            ("SELECT * FROM film WHERE rating = 'G'", "G"),
            ("SELECT * FROM film WHERE rating = 'PG'", "PG"),
            ("SELECT * FROM film WHERE rating = 'PG-13'", "PG-13"),
            ("SELECT * FROM film WHERE rating = 'R'", "R"),
            ("SELECT * FROM film WHERE rating = 'NC-17'", "NC-17"),
        ],
    )
    def test_sql_rating_extraction_parametrized(self, sql, expected_rating):
        """Parametrized test for rating extraction from SQL."""
        assert extract_rating_from_sql(sql) == expected_rating

    def test_pg_query_rejects_g_sql(self):
        """Critical test: PG query should reject G-rated SQL."""
        from mcp_server.services.cache.constraint_extractor import extract_constraints

        constraints = extract_constraints("Top 10 actors in PG films")
        sql = "SELECT * FROM film WHERE rating = 'G'"

        result = validate_sql_constraints(sql, constraints)

        assert result.is_valid is False
        assert result.mismatches[0].expected == "PG"
        assert result.mismatches[0].found == "G"

    def test_g_query_rejects_r_sql(self):
        """Critical test: G query should reject R-rated SQL."""
        from mcp_server.services.cache.constraint_extractor import extract_constraints

        constraints = extract_constraints("Top 10 actors in G films")
        sql = "SELECT * FROM film WHERE rating = 'R'"

        result = validate_sql_constraints(sql, constraints)

        assert result.is_valid is False
        assert result.mismatches[0].expected == "G"
        assert result.mismatches[0].found == "R"
