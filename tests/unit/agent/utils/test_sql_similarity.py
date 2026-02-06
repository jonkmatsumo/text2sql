"""Tests for SQL similarity utility."""

import pytest  # noqa: F401

from agent.utils.sql_similarity import compute_sql_similarity


def test_similarity_identical():
    """Test identical queries return 1.0."""
    sql = "SELECT * FROM users"
    assert compute_sql_similarity(sql, sql) == 1.0


def test_similarity_whitespace():
    """Test whitespace differences are ignored."""
    sql1 = "SELECT * FROM users"
    sql2 = "SELECT *   FROM   users"
    # AST matches
    assert compute_sql_similarity(sql1, sql2) == 1.0


def test_similarity_typo_fix():
    """Test distinct queries return fractional similarity."""
    sql1 = "SELECT naame FROM users"
    sql2 = "SELECT name FROM users"
    # Tables same (1.0 * 0.7) + Cols (0.0 * 0.3) -> 0.7
    score = compute_sql_similarity(sql1, sql2)
    assert 0.6 <= score <= 0.8


def test_similarity_drift_table():
    """Test table drift penalizes score."""
    sql1 = "SELECT * FROM users"
    sql2 = "SELECT * FROM orders"
    # Tables disjoint. Score 0.0 for tables.
    # Columns empty (Select *). Score 1.0 for columns.
    # Total: 0.7*0 + 0.3*1 = 0.3
    assert abs(compute_sql_similarity(sql1, sql2) - 0.3) < 0.01


def test_similarity_add_filter():
    """Test adding filters maintains high similarity."""
    sql1 = "SELECT * FROM users"
    sql2 = "SELECT * FROM users WHERE id > 5"
    # Tables match.
    assert compute_sql_similarity(sql1, sql2) >= 0.7


def test_similarity_parse_error():
    """Test parse errors return 0.0."""
    assert compute_sql_similarity("SELECT * FROM", "SELECT * FROM users") == 0.0
