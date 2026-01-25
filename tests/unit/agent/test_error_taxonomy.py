"""Unit tests for error taxonomy classification."""

from agent.taxonomy.error_taxonomy import (
    ERROR_TAXONOMY,
    ErrorCategory,
    classify_error,
    generate_correction_strategy,
)


class TestClassifyError:
    """Tests for error classification."""

    def test_classify_aggregation_misuse(self):
        """Test classification of aggregation errors."""
        error = "column 'name' must appear in the GROUP BY clause"
        category_key, category = classify_error(error)
        assert category_key == "AGGREGATION_MISUSE"
        assert "GROUP BY" in category.strategy

    def test_classify_missing_join(self):
        """Test classification of missing join errors."""
        error = "missing FROM-clause entry for table 'orders'"
        category_key, category = classify_error(error)
        assert category_key == "MISSING_JOIN"

    def test_classify_type_mismatch(self):
        """Test classification of type mismatch errors."""
        error = "operator does not exist: integer = text"
        category_key, category = classify_error(error)
        assert category_key == "TYPE_MISMATCH"
        assert "CAST" in category.strategy or "cast" in category.strategy.lower()

    def test_classify_ambiguous_column(self):
        """Test classification of ambiguous column errors."""
        error = "column reference 'id' is ambiguous"
        category_key, category = classify_error(error)
        assert category_key == "AMBIGUOUS_COLUMN"

    def test_classify_syntax_error(self):
        """Test classification of syntax errors."""
        error = "syntax error at or near 'SELCT'"
        category_key, category = classify_error(error)
        assert category_key == "SYNTAX_ERROR"

    def test_classify_null_handling(self):
        """Test classification of null handling errors."""
        error = "division by zero"
        category_key, category = classify_error(error)
        assert category_key == "NULL_HANDLING"
        assert "NULLIF" in category.strategy or "COALESCE" in category.strategy

    def test_classify_subquery_error(self):
        """Test classification of subquery errors."""
        error = "subquery must return only one column"
        category_key, category = classify_error(error)
        assert category_key == "SUBQUERY_ERROR"

    def test_classify_permission_denied(self):
        """Test classification of permission errors."""
        error = "permission denied for table sensitive_data"
        category_key, category = classify_error(error)
        assert category_key == "PERMISSION_DENIED"

    def test_classify_function_error(self):
        """Test classification of function errors."""
        error = "function date_trnc(text, timestamp) does not exist"
        category_key, category = classify_error(error)
        assert category_key == "FUNCTION_ERROR"

    def test_classify_limit_exceeded(self):
        """Test classification of limit exceeded errors."""
        error = "Result set too large (5000 rows)."
        category_key, category = classify_error(error)
        assert category_key == "LIMIT_EXCEEDED"

    def test_classify_date_time_error(self):
        """Test classification of date/time errors."""
        # Use a pattern that specifically matches DATE_TIME_ERROR
        error = "date/time field value out of range: '2024-13-45'"
        category_key, category = classify_error(error)
        assert category_key == "DATE_TIME_ERROR"

    def test_classify_unknown_error(self):
        """Test classification of unknown errors."""
        error = "some completely random and unexpected error message"
        category_key, category = classify_error(error)
        assert category_key == "UNKNOWN"
        assert "Analyze" in category.strategy

    def test_case_insensitive_matching(self):
        """Test that error matching is case insensitive."""
        error = "COLUMN 'NAME' MUST APPEAR IN THE GROUP BY CLAUSE"
        category_key, _ = classify_error(error)
        assert category_key == "AGGREGATION_MISUSE"


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

        # Aggregation misuse has an example fix
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
            assert isinstance(category, ErrorCategory)
            assert category.name is not None
            assert isinstance(category.patterns, list)
            assert len(category.patterns) > 0
            assert category.strategy is not None

    def test_category_count(self):
        """Test that we have expected number of categories."""
        # Should have at least 10 categories
        assert len(ERROR_TAXONOMY) >= 10
