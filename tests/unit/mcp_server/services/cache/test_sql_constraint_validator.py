"""Unit tests for SQL constraint validation."""

from mcp_server.services.cache.constraint_extractor import QueryConstraints
from mcp_server.services.cache.sql_constraint_validator import (
    extract_limit_from_sql,
    validate_sql_constraints,
)


class TestExtractLimitFromSql:
    """Tests for extract_limit_from_sql function."""

    def test_extract_limit_10(self, schema_fixture):
        """Test extraction of LIMIT 10."""
        sql = f"SELECT * FROM {schema_fixture.valid_table} LIMIT 10"
        assert extract_limit_from_sql(sql) == 10

    def test_extract_limit_5(self, schema_fixture):
        """Test extraction of LIMIT 5."""
        sql = f"SELECT * FROM {schema_fixture.valid_table} ORDER BY title LIMIT 5"
        assert extract_limit_from_sql(sql) == 5

    def test_no_limit(self, schema_fixture):
        """Test when no LIMIT exists."""
        sql = f"SELECT * FROM {schema_fixture.valid_table}"
        assert extract_limit_from_sql(sql) is None


class TestValidateSqlConstraints:
    """Tests for validate_sql_constraints function."""

    def test_valid_no_constraints(self, schema_fixture):
        """Test validation passes when no constraints specified."""
        sql = f"SELECT * FROM {schema_fixture.valid_table}"
        constraints = QueryConstraints()

        result = validate_sql_constraints(sql, constraints)

        assert result.is_valid is True

    def test_valid_limit_match(self, schema_fixture):
        """Test validation passes when limit matches."""
        sql = f"SELECT * FROM {schema_fixture.valid_table} LIMIT 10"
        constraints = QueryConstraints(limit=10)

        result = validate_sql_constraints(sql, constraints)

        assert result.is_valid is True

    def test_invalid_limit_mismatch(self, schema_fixture):
        """Test validation fails when limit doesn't match."""
        sql = f"SELECT * FROM {schema_fixture.valid_table} LIMIT 5"
        constraints = QueryConstraints(limit=10, include_ties=False)

        result = validate_sql_constraints(sql, constraints)

        assert result.is_valid is False
        assert result.mismatches[0].constraint_type == "limit"

    def test_limit_mismatch_allowed_with_ties(self, schema_fixture):
        """Test limit mismatch is allowed when include_ties is True."""
        sql = f"SELECT * FROM {schema_fixture.valid_table} LIMIT 15"  # More than 10 due to ties
        constraints = QueryConstraints(limit=10, include_ties=True)

        result = validate_sql_constraints(sql, constraints)

        # With ties, we allow different limits
        assert result.is_valid is True
