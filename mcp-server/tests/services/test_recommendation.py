from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.models import QueryPair
from mcp_server.services.recommendation.service import RecommendationService


@pytest.fixture
def mock_registry():
    """Mock the RegistryService for recommendation tests."""
    with patch("mcp_server.services.recommendation.service.RegistryService") as mock:
        mock.lookup_semantic = AsyncMock()
        yield mock


@pytest.mark.asyncio
async def test_recommend_ranking_priority(mock_registry):
    """Test that verified examples are prioritized over seeded ones."""
    # Setup
    verified_ex = QueryPair(
        signature_key="SIG1",
        tenant_id=1,
        fingerprint="F1",
        question="Q1",
        sql_query="S1",
        status="verified",
        roles=["example"],
    )
    seeded_ex = QueryPair(
        signature_key="SIG2",
        tenant_id=1,
        fingerprint="F2",
        question="Q2",
        sql_query="S2",
        status="seeded",
        roles=["example"],
    )

    # lookup_semantic called twice: once for verified, once for seeded
    mock_registry.lookup_semantic.side_effect = [
        [verified_ex],  # Results for status="verified"
        [seeded_ex],  # Results for status="seeded"
    ]

    # Execute
    result = await RecommendationService.recommend_examples("test", 1, limit=2)

    # Verify
    assert len(result.examples) == 2
    assert result.examples[0].source == "approved"
    assert result.examples[1].source == "seeded"
    assert not result.fallback_used


@pytest.mark.asyncio
async def test_recommend_diversity(mock_registry):
    """Test that only one example per canonical group (fingerprint) is picked."""
    # Setup: Two verified examples with same fingerprint
    ex1 = QueryPair(
        signature_key="SIG1",
        tenant_id=1,
        fingerprint="SAME_F",
        question="Q1",
        sql_query="S1",
        status="verified",
        roles=["example"],
    )
    ex2 = QueryPair(
        signature_key="SIG2",
        tenant_id=1,
        fingerprint="SAME_F",
        question="Q2",
        sql_query="S2",
        status="verified",
        roles=["example"],
    )

    mock_registry.lookup_semantic.side_effect = [
        [ex1, ex2],  # Both verified
        [],  # No seeded
        [],  # No history/fallback
    ]

    # Execute
    result = await RecommendationService.recommend_examples("test", 1, limit=2)

    # Verify: only 1 picked due to same fingerprint
    assert len(result.examples) == 1
    assert result.examples[0].canonical_group_id == "SAME_F"


@pytest.mark.asyncio
async def test_recommend_fallback(mock_registry):
    """Test that fallback is used when primary results are insufficient."""
    # Setup: No verified or seeded examples
    mock_registry.lookup_semantic.side_effect = [
        [],  # verified
        [],  # seeded
        [  # fallback (interactions)
            QueryPair(
                signature_key="HIST1",
                tenant_id=1,
                fingerprint="HF1",
                question="HQ1",
                sql_query="HS1",
                status="unverified",
                roles=["interaction"],
            )
        ],
    ]

    # Execute
    result = await RecommendationService.recommend_examples("test", 1, limit=1)

    # Verify
    assert len(result.examples) == 1
    assert result.examples[0].source == "fallback"
    assert result.fallback_used
