from mcp_server.services.recommendation.explanation import RecommendationExplanation


def test_explanation_serialization_defaults():
    """Test default values of RecommendationExplanation during serialization."""
    explanation = RecommendationExplanation()
    data = explanation.to_dict()

    assert data["selection_summary"]["total_candidates"] == 0
    assert data["filtering"]["tombstoned_removed"] == 0
    assert data["diversity"]["enabled"] is False
    assert data["pins"]["enabled"] is True
    assert data["fallback"]["used"] is False


def test_explanation_serialization_populated():
    """Test serialization of a populated RecommendationExplanation."""
    explanation = RecommendationExplanation(
        selection_summary={"total_candidates": 10, "returned_count": 3},
        filtering={"tombstoned_removed": 2},
        diversity={"enabled": True, "applied": True, "effects": {"verified_floor_applied": True}},
        pins={"selected_count": 1, "matched_rules": ["rule-1"]},
        fallback={"enabled": True, "used": False, "shortage_count": 0},
    )
    data = explanation.to_dict()

    assert data["selection_summary"]["total_candidates"] == 10
    assert data["selection_summary"]["returned_count"] == 3
    assert data["filtering"]["tombstoned_removed"] == 2
    assert data["diversity"]["applied"] is True
    assert data["diversity"]["effects"]["verified_floor_applied"] is True
    assert data["pins"]["selected_count"] == 1
    assert data["pins"]["matched_rules"] == ["rule-1"]
    assert data["fallback"]["enabled"] is True
