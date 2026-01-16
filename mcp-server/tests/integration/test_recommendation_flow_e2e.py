from unittest.mock import patch

import pytest
from mcp_server.models import QueryPair
from mcp_server.services.recommendation.config import RecommendationConfig
from mcp_server.tools.recommend_examples import handler


def create_config(**kwargs):
    """Create a RecommendationConfig with overrides."""
    defaults = {
        "limit_default": 3,
        "candidate_multiplier": 5,
        "fallback_enabled": True,
        "fallback_threshold": 0.85,
        "status_priority": ["verified", "seeded"],
        "exclude_tombstoned": True,
        "stale_max_age_days": 0,
        "diversity_enabled": True,
        "diversity_max_per_source": 1,
        "diversity_min_verified": 1,
        "safety_enabled": True,
        "safety_max_pattern_length": 50,
        "safety_blocklist_regex": "BLOCKME",
        "safety_require_sanitizable": True,
    }
    defaults.update(kwargs)
    return RecommendationConfig(**defaults)


def make_qp(question, fingerprint, status="verified", sql="SELECT 1", roles=None):
    """Create a QueryPair fixture."""
    return QueryPair(
        signature_key=f"sig_{fingerprint}",
        tenant_id=1,
        fingerprint=fingerprint,
        question=question,
        sql_query=sql,
        status=status,
        roles=roles or ["example"],
    )


@pytest.mark.asyncio
async def test_recommendation_flow_e2e_happy_path():
    """E2E integration test for the recommendation flow through the tool boundary.

    Covers:
    - Multiple candidate sources
    - Safety and Validity filtering
    - Pin application
    - Diversity enforcement
    - Explanation metadata
    """
    config = create_config()

    # Fixtures
    # 1. Verified candidates: 2 valid, 1 invalid (missing SQL), 1 unsafe (long question)
    v1 = make_qp("Standard question", "V1", "verified")
    v2 = make_qp("Another valid", "V2", "verified")
    v3 = make_qp("Missing SQL", "V3", "verified", sql="")
    v4 = make_qp(
        "Question that is way too long for the safety policy to allow it", "V4", "verified"
    )

    # 2. Seeded candidates: 2 valid (one will be capped)
    s1 = make_qp("Seeded question 1", "S1", "seeded")
    s2 = make_qp("Seeded question 2", "S2", "seeded")

    # 3. Fallback candidate (interaction)
    f1 = make_qp("Fallback interaction", "F1", "unverified", roles=["interaction"])

    # 4. Pinned Example (matches "Standard question" via mock pin rule)
    p1 = make_qp("Pinned question", "P1", "verified")

    from unittest.mock import MagicMock

    mock_pin_rule = MagicMock()
    mock_pin_rule.id = 101
    mock_pin_rule.priority = 10
    mock_pin_rule.match_type = "exact"
    mock_pin_rule.match_value = "Standard question"
    mock_pin_rule.registry_example_ids = ["sig_P1"]

    # Patching
    with patch("mcp_server.services.recommendation.service.RECO_CONFIG", config), patch(
        "mcp_server.services.registry.RegistryService.lookup_semantic"
    ) as mock_lookup, patch(
        "mcp_server.services.registry.RegistryService.fetch_by_signatures"
    ) as mock_fetch, patch(
        "dal.postgres.pinned_recommendations." "PostgresPinnedRecommendationStore.list_rules"
    ) as mock_list_rules:

        # Setup Mocks
        # 1: approved, 2: seeded, 3: history
        mock_lookup.side_effect = [
            [v1, v2, v3, v4],  # approved
            [s1, s2],  # seeded
            [f1],  # history
        ]

        mock_fetch.return_value = [p1]
        mock_list_rules.return_value = [mock_pin_rule]

        # Execute
        # Setting limit to 4 to trigger both diversity caps and fallback
        response = await handler(
            query="Standard question", tenant_id=1, limit=4, enable_fallback=True
        )

        # Assertions
        assert "examples" in response
        examples = response["examples"]

        # Final Expected: [P1, V1, S1, F1]
        assert len(examples) == 4
        ids = [e["canonical_group_id"] for e in examples]
        assert "P1" in ids
        assert "V1" in ids
        assert "S1" in ids
        assert "F1" in ids

        # Verify explanation
        assert "explanation" in response
        exp = response["explanation"]

        # Filtering
        assert exp["filtering"]["missing_fields_removed"] == 1
        assert exp["filtering"]["safety_removed"] == 1

        # Selection Summary
        # 4 approved + 2 seeded + 1 interaction + 1 pinned = 8
        assert exp["selection_summary"]["total_candidates"] == 8
        assert exp["selection_summary"]["counts_by_source"]["interactions"] == 1

        # Pins
        assert exp["pins"]["selected_count"] == 1
        assert "101" in exp["pins"]["matched_rules"]

        # Diversity
        assert exp["diversity"]["applied"] is True
        assert exp["diversity"]["effects"]["verified_floor_applied"] is True
        assert exp["diversity"]["effects"]["source_caps_applied"]["approved"] == 1
        assert exp["diversity"]["effects"]["source_caps_applied"]["seeded"] == 1

        # Fallback
        assert exp["fallback"]["used"] is True


@pytest.mark.asyncio
async def test_recommendation_flow_fallback_disabled():
    """Test the recommendation flow when fallback is explicitly disabled."""
    # Config with shortage
    config = create_config(diversity_enabled=False)

    # Only 1 candidate available total
    v1 = make_qp("Only one", "V1", "verified")

    with patch("mcp_server.services.recommendation.service.RECO_CONFIG", config), patch(
        "mcp_server.services.registry.RegistryService.lookup_semantic"
    ) as mock_lookup, patch(
        "dal.postgres.pinned_recommendations." "PostgresPinnedRecommendationStore.list_rules"
    ) as mock_list_rules:

        mock_lookup.side_effect = [
            [v1],  # approved
            [],  # seeded
            [],  # history
        ]
        mock_list_rules.return_value = []

        # Execute with enable_fallback=False
        response = await handler(query="Only one", tenant_id=1, limit=3, enable_fallback=False)

        # Assertions
        assert len(response["examples"]) == 1
        assert response["explanation"]["fallback"]["enabled"] is False
        assert response["explanation"]["fallback"]["used"] is False
