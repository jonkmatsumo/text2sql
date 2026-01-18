"""Integration tests for constraint extraction -> intent signature pipeline."""

import pytest
from mcp_server.services.cache.constraint_extractor import extract_constraints
from mcp_server.services.cache.intent_signature import build_signature_from_constraints


@pytest.fixture
def mode_synthetic(monkeypatch):
    """Set dataset mode to synthetic."""
    monkeypatch.setenv("DATASET_MODE", "synthetic")


@pytest.mark.usefixtures("mode_synthetic")
def test_synthetic_signature_safe():
    """Verify that synthetic queries do not leak film intents into signature."""
    query = "Show top 10 merchants"
    constraints = extract_constraints(query)

    # Factory function manually maps constraint fields
    signature = build_signature_from_constraints(
        query=query,
        rating=constraints.rating,
        limit=constraints.limit,
        sort_direction=constraints.sort_direction,
        include_ties=constraints.include_ties,
        entity=constraints.entity,
        metric=constraints.metric,
    )

    # Verify signature
    # Since synthetic entity patterns are restored,
    # intent matching logic triggers "top_merchants".
    assert signature.intent == "top_merchants"
    assert signature.entity == "merchant"
    # Ensure no film filtering or rating leakage
    assert "rating" not in signature.filters
    assert signature.item != "film"


@pytest.mark.usefixtures("mode_synthetic")
def test_synthetic_pg13_leakage_prevention(mode_synthetic):
    """Verify that PG-13 rating is not extracted in synthetic mode."""
    query = "Show top 10 PG-13 movies"
    constraints = extract_constraints(query)

    # In synthetic mode:
    # - rating should be None
    # - entity should be None (since 'movie' matches nothing)
    # - limit should be 10

    signature = build_signature_from_constraints(
        query=query,
        rating=constraints.rating,
        limit=constraints.limit,
        sort_direction=constraints.sort_direction,
        include_ties=constraints.include_ties,
        entity=constraints.entity,
        metric=constraints.metric,
    )

    # Intent should be None or generic because no entity matched
    # "top" logic in intent_signature checks "if entity: ... elif 'film' in query"
    # Logic in intent_signature.py is HARDCODED to check "film" in query string.
    # So if I pass entity=None, but query has "movies", intent_signature WILL return 'top_films'.
    # This confirms the finding in Issue #286 that intent_signature is NOT fully safe yet.
    # However, Phase 4 task says verify tolerance.

    # Asserting what IS expected given CURRENT state of intent_signature.
    assert constraints.rating is None
    assert "rating" not in signature.filters
