"""Unit tests for cache extraction and validation logic."""

import pytest
from mcp_server.services.cache.constraint_extractor import extract_constraints
from mcp_server.services.cache.intent_signature import build_signature_from_constraints
from mcp_server.services.cache.sql_constraint_validator import validate_sql_constraints

pytestmark = pytest.mark.pagila


@pytest.fixture(autouse=True)
def skip_if_not_pagila(dataset_mode):
    """Skip these tests if we are not explicitly running for Pagila."""
    import os

    if os.getenv("RUN_PAGILA_TESTS", "0") == "1":
        return
    if dataset_mode != "pagila":
        pytest.skip("Skipping Pagila tests in synthetic mode")


def test_extract_constraints():
    """Test extracting linguistic constraints from a natural language query."""
    query = "Top 10 rated PG movies"
    constraints = extract_constraints(query)
    assert constraints.limit == 10
    assert constraints.rating == "PG"
    assert constraints.entity == "film"


def test_validate_sql_constraints_pass():
    """Test that valid SQL correctly matches extracted constraints."""
    sql = "SELECT * FROM film WHERE rating = 'PG' LIMIT 10"
    constraints = extract_constraints("Top 10 rated PG movies")
    result = validate_sql_constraints(sql, constraints)
    assert result.is_valid is True


def test_validate_sql_constraints_fail_rating():
    """Test that SQL with mismatched rating fails validation."""
    sql = "SELECT * FROM film WHERE rating = 'G' LIMIT 10"
    constraints = extract_constraints("Top 10 rated PG movies")
    result = validate_sql_constraints(sql, constraints)
    assert result.is_valid is False
    assert any(m.constraint_type == "rating" for m in result.mismatches)


def test_validate_sql_constraints_fail_limit():
    """Test that SQL with mismatched limit fails validation."""
    sql = "SELECT * FROM film WHERE rating = 'PG' LIMIT 5"
    constraints = extract_constraints("Top 10 rated PG movies")
    result = validate_sql_constraints(sql, constraints)
    assert result.is_valid is False
    assert any(m.constraint_type == "limit" for m in result.mismatches)


def test_intent_signature():
    """Test that semantically identical queries produce the same intent signature."""
    constraints = extract_constraints("Top 10 rated PG movies")
    sig = build_signature_from_constraints(
        "Top 10 rated PG movies",
        rating=constraints.rating,
        limit=constraints.limit,
        entity=constraints.entity,
    )
    key1 = sig.compute_key()

    sig2 = build_signature_from_constraints(
        "Give me the top 10 movies rated PG", rating="PG", limit=10, entity="film"
    )
    key2 = sig2.compute_key()

    assert key1 == key2
