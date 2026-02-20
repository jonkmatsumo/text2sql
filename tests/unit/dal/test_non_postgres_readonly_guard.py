"""Unit tests for non-Postgres read-only mutation keyword guard."""

import pytest

from dal.util.read_only import validate_no_mutation_keywords


def test_validate_no_mutation_keywords_allows_select():
    """Allow pure SELECT statements."""
    validate_no_mutation_keywords("SELECT id, name FROM users WHERE id = 1")


def test_validate_no_mutation_keywords_ignores_comments():
    """Ignore mutation keywords that appear only inside comments."""
    validate_no_mutation_keywords(
        """
        SELECT 1
        -- INSERT INTO users VALUES (1)
        /* MERGE INTO users */
        """
    )


def test_validate_no_mutation_keywords_rejects_insert_disguised_with_formatting():
    """Reject keyword-split INSERT statements after comment stripping."""
    with pytest.raises(PermissionError, match="INSERT"):
        validate_no_mutation_keywords("IN/* spacing */SERT INTO users(id) VALUES (1)")


def test_validate_no_mutation_keywords_rejects_merge():
    """Reject MERGE statements."""
    with pytest.raises(PermissionError, match="MERGE"):
        validate_no_mutation_keywords(
            "MERGE INTO users u USING staging s ON u.id = s.id WHEN MATCHED THEN UPDATE SET id = 1"
        )


def test_validate_no_mutation_keywords_rejects_call():
    """Reject procedure calls."""
    with pytest.raises(PermissionError, match="CALL"):
        validate_no_mutation_keywords("CALL refresh_materialized_views()")
