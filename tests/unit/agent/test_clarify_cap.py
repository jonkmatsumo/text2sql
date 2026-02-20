"""Tests for clarify loop iteration cap routing."""

from agent.graph import MAX_CLARIFY_ROUNDS, route_after_router
from agent.models.termination import TerminationReason


def test_route_after_router_caps_clarify_to_synthesize():
    """Ambiguous requests at cap should stop clarifying and synthesize a response."""
    state = {
        "messages": [],
        "ambiguity_type": "UNCLEAR_SCHEMA_REFERENCE",
        "clarify_count": MAX_CLARIFY_ROUNDS,
    }

    result = route_after_router(state)

    assert result == "synthesize"
    assert state["termination_reason"] == TerminationReason.INVALID_REQUEST
    assert state["error_category"] == "invalid_request"
    assert "can't resolve the ambiguity" in state["error"].lower()


def test_route_after_router_allows_clarify_below_cap():
    """Ambiguous requests below cap should continue to clarify."""
    state = {
        "messages": [],
        "ambiguity_type": "UNCLEAR_SCHEMA_REFERENCE",
        "clarify_count": MAX_CLARIFY_ROUNDS - 1,
    }

    result = route_after_router(state)

    assert result == "clarify"


def test_route_after_router_defaults_missing_count_to_clarify():
    """Missing counter defaults to zero and should still clarify."""
    state = {
        "messages": [],
        "ambiguity_type": "UNCLEAR_SCHEMA_REFERENCE",
    }

    result = route_after_router(state)

    assert result == "clarify"


def test_route_after_router_clear_query_still_routes_plan():
    """Clear query should route to plan regardless of clarify count."""
    state = {
        "messages": [],
        "ambiguity_type": None,
        "clarify_count": MAX_CLARIFY_ROUNDS,
    }

    result = route_after_router(state)

    assert result == "plan"
