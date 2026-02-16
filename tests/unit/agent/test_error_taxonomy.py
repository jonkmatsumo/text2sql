"""Unit tests for error taxonomy classification."""

from agent.taxonomy.error_taxonomy import (
    ERROR_TAXONOMY,
    ErrorTaxonomyEntry,
    classify_error,
    generate_correction_strategy,
)
from common.models.error_metadata import ErrorCategory


class TestClassifyError:
    """Tests for error classification."""

    def test_classify_aggregation_misuse(self):
        """Test classification of aggregation errors."""
        error = "column 'name' must appear in the GROUP BY clause"
        category_key, category = classify_error(error)
        assert category_key == ErrorCategory.INVALID_REQUEST.value
        assert "GROUP BY" in category.strategy

    def test_classify_missing_join(self):
        """Test classification of missing join errors."""
        error = "missing FROM-clause entry for table 'orders'"
        category_key, category = classify_error(error)
        assert category_key == ErrorCategory.INVALID_REQUEST.value
        assert "JOIN" in category.strategy

    def test_classify_type_mismatch(self):
        """Test classification of type mismatch errors."""
        error = "operator does not exist: integer = text"
        category_key, category = classify_error(error)
        assert category_key == ErrorCategory.INVALID_REQUEST.value
        # The strategy mentions CAST
        assert "CAST" in category.strategy

    def test_classify_ambiguous_column(self):
        """Test classification of ambiguous column errors."""
        error = "column reference 'id' is ambiguous"
        category_key, category = classify_error(error)
        assert category_key == ErrorCategory.INVALID_REQUEST.value

    def test_classify_syntax_error(self):
        """Test classification of syntax errors."""
        error = "syntax error at or near 'SELCT'"
        category_key, category = classify_error(error)
        assert category_key == ErrorCategory.SYNTAX.value

    def test_classify_null_handling(self):
        """Test classification of null handling errors."""
        error = "division by zero"
        category_key, category = classify_error(error)
        assert category_key == ErrorCategory.INVALID_REQUEST.value

    def test_classify_subquery_error(self):
        """Test classification of subquery errors."""
        error = "subquery must return only one column"
        category_key, category = classify_error(error)
        assert category_key == ErrorCategory.INVALID_REQUEST.value

    def test_classify_permission_denied(self):
        """Test classification of permission errors."""
        error = "permission denied for table sensitive_data"
        category_key, category = classify_error(error)
        assert category_key == ErrorCategory.UNAUTHORIZED.value

    def test_classify_function_error(self):
        """Test classification of function errors."""
        error = "function date_trnc(text, timestamp) does not exist"
        category_key, category = classify_error(error)
        assert category_key == ErrorCategory.INVALID_REQUEST.value

    def test_classify_limit_exceeded(self):
        """Test classification of limit exceeded errors."""
        error = "Result set too large (5000 rows)."
        category_key, category = classify_error(error)
        assert category_key == ErrorCategory.RESOURCE_EXHAUSTED.value

    def test_classify_date_time_error(self):
        """Test classification of date/time errors."""
        error = "date/time field value out of range: '2024-13-45'"
        category_key, category = classify_error(error)
        assert category_key == ErrorCategory.INVALID_REQUEST.value

    def test_classify_unknown_error(self):
        """Test classification of unknown errors."""
        error = "some completely random and unexpected error message"
        category_key, category = classify_error(error)
        assert category_key == ErrorCategory.UNKNOWN.value
        assert "Analyze" in category.strategy

    def test_case_insensitive_matching(self):
        """Test that error matching is case insensitive."""
        error = "COLUMN 'NAME' MUST APPEAR IN THE GROUP BY CLAUSE"
        category_key, _ = classify_error(error)
        assert category_key == ErrorCategory.INVALID_REQUEST.value


class TestGenerateCorrectionStrategy:
    """Tests for correction strategy generation."""

    def test_generates_formatted_strategy(self):
        """Test that strategy is properly formatted."""
        strategy = generate_correction_strategy(
            error_message="column 'name' must appear in the GROUP BY clause",
            failed_sql="SELECT name, COUNT(*) FROM customers",
            schema_context="Table: customers (id, name, email)",
        )

        assert "## Error Classification" in strategy
        assert "### Error Message" in strategy
        assert "### Failed SQL" in strategy
        assert "### Correction Strategy" in strategy
        assert "### Instructions" in strategy

    def test_includes_example_fix_when_available(self):
        """Test that example fix is included when category has one."""
        strategy = generate_correction_strategy(
            error_message="column 'name' must appear in the GROUP BY clause",
            failed_sql="SELECT name, COUNT(*) FROM customers",
        )

        # INVALID_REQUEST has an example fix
        assert "### Example Fix" in strategy

    def test_handles_empty_error(self):
        """Test handling of empty error message."""
        strategy = generate_correction_strategy(
            error_message="",
            failed_sql="SELECT * FROM test",
        )

        # Should still generate a strategy (unknown category)
        assert "## Error Classification" in strategy


class TestErrorTaxonomy:
    """Tests for ERROR_TAXONOMY structure."""

    def test_all_categories_have_required_fields(self):
        """Test that all categories have required fields."""
        for key, category in ERROR_TAXONOMY.items():
            assert isinstance(key, ErrorCategory)
            assert isinstance(category, ErrorTaxonomyEntry)
            assert category.name is not None
            assert isinstance(category.patterns, list)
            assert len(category.patterns) > 0
            assert category.strategy is not None

    def test_category_count(self):
        """Test that we have expected number of categories."""
        # We consolidated categories, so count is smaller now.
        # Currently: INVALID_REQUEST, SYNTAX, UNAUTHORIZED, RESOURCE_EXHAUSTED,
        # TIMEOUT, SCHEMA_DRIFT. (That's 6).
        assert len(ERROR_TAXONOMY) >= 5
