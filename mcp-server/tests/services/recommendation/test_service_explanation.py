from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.models import QueryPair
from mcp_server.services.recommendation.config import RecommendationConfig
from mcp_server.services.recommendation.service import RecommendationService


@pytest.fixture
def mock_registry():
    """Mock the RegistryService."""
    with patch("mcp_server.services.recommendation.service.RegistryService") as mock:
        mock.lookup_semantic = AsyncMock()
        mock.fetch_by_signatures = AsyncMock(return_value=[])
        yield mock


@pytest.fixture(autouse=True)
def mock_pin_store():
    """Mock the PostgresPinnedRecommendationStore."""
    with patch(
        "mcp_server.dal.postgres.pinned_recommendations.PostgresPinnedRecommendationStore"
    ) as mock:
        mock.return_value.list_rules = AsyncMock(return_value=[])
        yield mock


def make_qp(fingerprint, status):
    """Create a QueryPair."""
    return QueryPair(
        signature_key=f"sig_{fingerprint}_{status}",
        tenant_id=1,
        fingerprint=fingerprint,
        question=f"q_{fingerprint}",
        sql_query=f"s_{fingerprint}",
        status=status,
        roles=["example"],
    )


@pytest.mark.asyncio
async def test_explanation_population_basic(mock_registry):
    """Test basic explanation population."""
    # Setup
    v1 = make_qp("F1", "verified")
    s1 = make_qp("F2", "seeded")
    mock_registry.lookup_semantic.side_effect = [[v1], [s1], []]

    # Execute
    result = await RecommendationService.recommend_examples("test", 1, limit=2)

    # Verify
    assert result.explanation is not None
    exp = result.explanation
    assert exp.selection_summary.total_candidates == 2
    assert exp.selection_summary.returned_count == 2
    assert exp.selection_summary.counts_by_source["approved"] == 1
    assert exp.selection_summary.counts_by_source["seeded"] == 1
    assert exp.selection_summary.counts_by_status["approved"] == 1
    assert exp.selection_summary.counts_by_status["seeded"] == 1


@pytest.mark.asyncio
async def test_explanation_filtering_counters(mock_registry):
    """Test filtering counters in explanation."""
    # Setup: 1 tombstoned, 1 missing fields
    tomb = make_qp("F1", "tombstoned")
    incomplete = make_qp("F2", "verified")
    incomplete.sql_query = ""
    valid = make_qp("F3", "verified")

    mock_registry.lookup_semantic.side_effect = [[tomb, incomplete, valid], [], []]

    # Execute
    result = await RecommendationService.recommend_examples("test", 1, limit=1)

    # Verify
    exp = result.explanation
    assert exp.filtering.tombstoned_removed == 1
    assert exp.filtering.missing_fields_removed == 1
    assert exp.selection_summary.returned_count == 1


def create_config(**kwargs):
    """Create RecommendationConfig."""
    defaults = {
        "limit_default": 3,
        "candidate_multiplier": 2,
        "fallback_enabled": True,
        "fallback_threshold": 0.85,
        "status_priority": ["verified", "seeded"],
        "exclude_tombstoned": True,
        "stale_max_age_days": 0,
        "diversity_enabled": False,
        "diversity_max_per_source": -1,
        "diversity_min_verified": 0,
        "safety_enabled": False,
        "safety_max_pattern_length": 100,
        "safety_blocklist_regex": None,
        "safety_require_sanitizable": True,
    }
    defaults.update(kwargs)
    return RecommendationConfig(**defaults)


@pytest.mark.asyncio
async def test_explanation_diversity_population(mock_registry):
    """Test diversity details in explanation."""
    # Setup
    v1 = make_qp("F1", "verified")
    v2 = make_qp("F2", "verified")

    config = create_config(
        diversity_enabled=True, diversity_max_per_source=1, diversity_min_verified=1
    )

    mock_registry.lookup_semantic.side_effect = [[v1, v2], [], []]

    with patch("mcp_server.services.recommendation.service.RECO_CONFIG", config):
        result = await RecommendationService.recommend_examples("test", 1, limit=2)

    # Verify
    exp = result.explanation
    assert exp.diversity.enabled is True
    assert exp.diversity.applied is True
    assert exp.diversity.effects.verified_floor_applied is True
    assert exp.diversity.effects.source_caps_applied["approved"] == 1


@pytest.mark.asyncio
async def test_explanation_fallback_population(mock_registry):
    """Test fallback details in explanation."""
    # Setup: No primary, 1 fallback
    f1 = make_qp("F1", "unverified")
    f1.roles = ["interaction"]
    mock_registry.lookup_semantic.side_effect = [[], [], [f1]]

    # Execute
    result = await RecommendationService.recommend_examples("test", 1, limit=1)

    # Verify
    exp = result.explanation
    assert exp.fallback.used is True
    assert exp.fallback.reason == "insufficient_verified_candidates"
    assert exp.selection_summary.counts_by_source["interactions"] == 1
