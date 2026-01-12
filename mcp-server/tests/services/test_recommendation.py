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


def make_qp(fingerprint, status):
    """Create a dummy QueryPair for testing."""
    return QueryPair(
        signature_key="sig",
        tenant_id=1,
        fingerprint=fingerprint,
        question="q",
        sql_query="s",
        status=status,
        roles=["example"],
    )


def test_diversity_policy_disabled():
    """Test that diversity policy returns candidates unchanged when disabled."""
    candidates = [make_qp("F1", "verified"), make_qp("F2", "seeded")]
    config = {"diversity_enabled": False}
    result = RecommendationService._apply_diversity_policy(candidates, 2, config)
    assert len(result) == 2
    assert result == candidates


def test_diversity_policy_cap_enforced():
    """Test that max_per_source cap is enforced."""
    # Input order preserved (V1, V2, S1, S2)
    candidates = [
        make_qp("F1", "verified"),
        make_qp("F2", "verified"),
        make_qp("F3", "seeded"),
        make_qp("F4", "seeded"),
    ]
    # Cap approved at 1
    config = {
        "diversity_enabled": True,
        "diversity_max_per_source": 1,
        "diversity_min_verified": 0,
    }

    result = RecommendationService._apply_diversity_policy(candidates, 4, config)

    # V1 (approved count=1). V2 skipped (count=1 >= max 1).
    # S1 (seeded count=1). S2 (seeded count=2).
    # Wait, max_per_source applies to ALL sources?
    # "3. Caps apply per source bucket." implies distinct caps?
    # Or "diversity_max_per_source: int" implies GLOBAL cap per source?
    # "diversity_max_per_source: int" -> single int.
    # So capped at X per bucket.

    # So V1 selected. V2 skipped.
    # S1 selected. S2 skipped (if S cap also 1).
    # Expected: [V1, S1].

    assert len(result) == 2
    assert result[0].fingerprint == "F1"
    assert result[1].fingerprint == "F3"


def test_diversity_policy_verified_floor():
    """Test that verified floor ensures selection of verified examples."""
    # Candidates: [S1, S2, V1, V2]
    # Note: _rank_candidates would sort V first, but _apply_diversity_policy takes ALREADY
    # sorted list. So if we feed it [S1, V1] manually (simulating ranker override or tie break),
    # Pass A should pick V1 if floor > 0.

    candidates = [
        make_qp("F1", "seeded"),
        make_qp("F2", "verified"),
        make_qp("F3", "seeded"),
    ]

    # Min verified 1. Max source 2.
    config = {
        "diversity_enabled": True,
        "diversity_max_per_source": 2,
        "diversity_min_verified": 1,
    }

    result = RecommendationService._apply_diversity_policy(candidates, 2, config)

    # Pass A:
    # S1: source seeded. Skipped.
    # V1 (F2): source approved. count=0 < min(1). Selected.
    # S2 (F3): seeding. Skipped.

    # Pass B:
    # S1 (F1): Not selected. Count seeded=0. Selected.
    # V1: Already selected.
    # S2: Limit already 2? NO, selected len is 2. Break.

    # Expected: [V1, S1] because order = selection order.
    # V1 selected in Pass A. S1 in Pass B.

    assert len(result) == 2
    assert result[0].fingerprint == "F2"  # Verified picked first due to floor logic
    assert result[1].fingerprint == "F1"  # Seeded picked second
