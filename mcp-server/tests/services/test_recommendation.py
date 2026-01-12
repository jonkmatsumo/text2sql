from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.models import QueryPair
from mcp_server.services.recommendation.config import RecommendationConfig
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
        signature_key=f"sig_{fingerprint}_{status}",
        tenant_id=1,
        fingerprint=fingerprint,
        question=f"q_{fingerprint}",
        sql_query=f"s_{fingerprint}",
        status=status,
        roles=["example"],
    )


def create_config(**kwargs):
    """Create RecommendationConfig with defaults."""
    from mcp_server.services.recommendation.config import RecommendationConfig

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
    }
    defaults.update(kwargs)
    return RecommendationConfig(**defaults)


def make_interaction_qp(fingerprint):
    """Create a dummy interaction QueryPair for testing."""
    qp = make_qp(fingerprint, "unverified")
    qp.roles = ["interaction"]
    return qp


@pytest.fixture
def diversity_pool():
    """Return a controlled pool of QueryPairs for diversity testing."""
    return {
        "v1": make_qp("F1", "verified"),
        "v2": make_qp("F2", "verified"),
        "v3": make_qp("F3", "verified"),
        "s1": make_qp("S1", "seeded"),
        "s2": make_qp("S2", "seeded"),
        "f1": make_interaction_qp("F4"),
        "f2": make_interaction_qp("F5"),
        "dup_v1": make_qp("F1", "verified"),  # Shares fingerprint with F1
    }


def test_diversity_policy_disabled():
    """Return candidates unchanged when diversity is disabled."""
    candidates = [make_qp("F1", "verified"), make_qp("F2", "seeded")]
    config = create_config(diversity_enabled=False)
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
    config = create_config(
        diversity_enabled=True,
        diversity_max_per_source=1,
        diversity_min_verified=0,
    )

    result = RecommendationService._apply_diversity_policy(candidates, 4, config)

    # V1 (approved count=1). V2 skipped (count=1 >= max 1).
    # S1 (seeded count=1). S2 skipped (if S cap also 1).
    # Expected: [V1, S1].

    assert len(result) == 2
    assert result[0].fingerprint == "F1"
    assert result[1].fingerprint == "F3"


def test_diversity_policy_verified_floor():
    """Test that verified floor ensures selection of verified examples."""
    candidates = [
        make_qp("F1", "seeded"),
        make_qp("F2", "verified"),
        make_qp("F3", "seeded"),
    ]

    # Min verified 1. Max source 2.
    config = create_config(
        diversity_enabled=True,
        diversity_max_per_source=2,
        diversity_min_verified=1,
    )

    result = RecommendationService._apply_diversity_policy(candidates, 2, config)

    # Expected: [V1, S1] because order = selection order.
    # V1 selected in Pass A. S1 in Pass B.

    assert len(result) == 2
    assert result[0].fingerprint == "F2"  # Verified picked first due to floor logic
    assert result[1].fingerprint == "F1"  # Seeded picked second


def test_diversity_invalid_config(caplog):
    """Test that invalid config disables diversity and logs warning."""
    import logging

    candidates = [make_qp("F1", "verified"), make_qp("F2", "seeded")]

    # Invalid max_per_source - need to pass as int for type safety if mocking,
    # but here we test runtime checks. Since RecommendationConfig is frozen,
    # we might be blocked by type checkers statically, but runtime is fine.
    # However, create_config uses kwargs unpacking so types aren't enforced at call time.

    # Unexpected type injection via kwargs
    # Note: Dataclasses don't enforce types at runtime, so this is valid for testing
    # strict type checks in logic.
    config = create_config(diversity_enabled=True, diversity_max_per_source="invalid")

    with caplog.at_level(logging.WARNING):
        result = RecommendationService._apply_diversity_policy(candidates, 2, config)

    assert len(result) == 2
    assert "Invalid diversity_max_per_source" in caplog.text

    # Invalid min_verified
    caplog.clear()
    config = create_config(diversity_enabled=True, diversity_min_verified=-5)

    with caplog.at_level(logging.WARNING):
        result = RecommendationService._apply_diversity_policy(candidates, 2, config)

    assert len(result) == 2
    assert "Invalid diversity_min_verified" in caplog.text


@pytest.mark.asyncio
async def test_config_override_integration(mock_registry):
    """Test config overrides on service behavior."""
    from mcp_server.services.recommendation.config import RecommendationConfig

    # 1. Test Multiplier
    custom_config = RecommendationConfig(
        limit_default=3,
        candidate_multiplier=10,
        fallback_enabled=True,
        fallback_threshold=0.85,
        status_priority=["verified", "seeded"],
        exclude_tombstoned=True,  # New
        stale_max_age_days=0,  # New
        diversity_enabled=False,
        diversity_max_per_source=-1,
        diversity_min_verified=0,
    )

    with patch("mcp_server.services.recommendation.service.RECO_CONFIG", custom_config):
        mock_registry.lookup_semantic.return_value = []
        await RecommendationService.recommend_examples("test", 1, limit=5)

        # Check calls to registry
        # Expected limit = 5 * 10 = 50
        calls = mock_registry.lookup_semantic.call_args_list
        # Verified lookup
        assert calls[0].kwargs["limit"] == 50
        # Seeded lookup
        assert calls[1].kwargs["limit"] == 50

    # 2. Test Fallback Threshold & Enabled
    # Case: Fallback enabled=False
    custom_config_disabled = RecommendationConfig(
        limit_default=3,
        candidate_multiplier=2,
        fallback_enabled=False,  # Disabled via config
        fallback_threshold=0.1,
        status_priority=["verified"],
        exclude_tombstoned=True,  # New
        stale_max_age_days=0,  # New
        diversity_enabled=False,
        diversity_max_per_source=-1,
        diversity_min_verified=0,
    )

    with patch("mcp_server.services.recommendation.service.RECO_CONFIG", custom_config_disabled):
        mock_registry.lookup_semantic.reset_mock()
        mock_registry.lookup_semantic.return_value = (
            []
        )  # Return empty to trigger fallback logic if enabled

        await RecommendationService.recommend_examples("test", 1, limit=5, enable_fallback=True)

        # Should NOT call fallback (verified + seeded = 2 calls)
        assert mock_registry.lookup_semantic.call_count == 2

    # Case: Fallback threshold
    custom_config_threshold = RecommendationConfig(
        limit_default=3,
        candidate_multiplier=2,
        fallback_enabled=True,
        fallback_threshold=0.99,
        status_priority=["verified"],
        exclude_tombstoned=True,  # New
        stale_max_age_days=0,  # New
        diversity_enabled=False,
        diversity_max_per_source=-1,
        diversity_min_verified=0,
    )

    with patch("mcp_server.services.recommendation.service.RECO_CONFIG", custom_config_threshold):
        mock_registry.lookup_semantic.reset_mock()
        # Primaries empty
        mock_registry.lookup_semantic.side_effect = [[], [], []]

        await RecommendationService.recommend_examples("test", 1, limit=5, enable_fallback=True)

        # Should call fallback with threshold 0.99
        calls = mock_registry.lookup_semantic.call_args_list
        assert len(calls) == 3
        assert calls[2].kwargs["threshold"] == 0.99
        assert calls[2].kwargs["role"] == "interaction"

    # 3. Test Status Priority
    # Change priority to favor seeded over verified
    custom_config_priority = RecommendationConfig(
        limit_default=3,
        candidate_multiplier=2,
        fallback_enabled=True,
        fallback_threshold=0.85,
        status_priority=["seeded", "verified"],  # Seeded (0) > Verified (1)
        exclude_tombstoned=True,  # New
        stale_max_age_days=0,  # New
        diversity_enabled=False,
        diversity_max_per_source=-1,
        diversity_min_verified=0,
    )

    # We test _rank_candidates directly or via public API
    v_ex = make_qp("F1", "verified")
    s_ex = make_qp("F2", "seeded")

    with patch("mcp_server.services.recommendation.service.RECO_CONFIG", custom_config_priority):
        ranked = RecommendationService._rank_candidates([v_ex, s_ex])
        # Seeded should be first
        assert ranked[0].status == "seeded"
        assert ranked[1].status == "verified"


# --- Phase 1: Validity Filtering Tests ---


@pytest.mark.asyncio
async def test_recommendation_excludes_tombstoned(mock_registry):
    """Test that tombstoned candidates are filtered out."""
    # 1. Verified Tombstoned
    tomb_ex = make_qp("f1", "tombstoned")
    valid_ex = make_qp("f2", "verified")

    mock_registry.lookup_semantic.side_effect = [[tomb_ex, valid_ex], [], []]

    result = await RecommendationService.recommend_examples("test", 1)

    assert len(result.examples) == 1
    assert result.examples[0].canonical_group_id == "f2"

    # 2. Fallback Tombstoned
    mock_registry.lookup_semantic.reset_mock()
    # Primaries empty, fallback has one tombstoned, one valid
    mock_registry.lookup_semantic.side_effect = [[], [], [tomb_ex, valid_ex]]

    result = await RecommendationService.recommend_examples("test", 1, enable_fallback=True)

    # Only valid fallback should be used
    assert len(result.examples) == 1
    assert result.examples[0].canonical_group_id == "f2"
    assert result.fallback_used is True


@pytest.mark.asyncio
async def test_recommendation_excludes_invalid_fields(mock_registry):
    """Test that candidates missing required fields are filtered out."""
    # Missing fingerprint
    no_fp = make_qp("fp_missing", "verified")
    no_fp.fingerprint = None

    # Missing SQL
    no_sql = make_qp("sql_missing", "verified")
    no_sql.sql_query = ""

    # Missing Question
    no_q = make_qp("q_missing", "verified")
    no_q.question = None

    # Valid
    valid = make_qp("valid", "verified")

    mock_registry.lookup_semantic.return_value = [no_fp, no_sql, no_q, valid]

    # Note: seeded will be empty due to return_value behavior if not side_effect with multiple calls
    # but simplest is just return valid list for all calls
    mock_registry.lookup_semantic.side_effect = None
    mock_registry.lookup_semantic.return_value = [no_fp, no_sql, no_q, valid]

    # We expect fetch limit * 2 calls or similar, but what matters is the result
    # It will fetch for verified, then seeded. Both return the mixed bag.
    # deduping will happen. valid(verified) + valid(seeded) -> deduped to 1 valid

    result = await RecommendationService.recommend_examples("test", 1)

    assert len(result.examples) == 1
    assert result.examples[0].canonical_group_id == "valid"


@pytest.mark.asyncio
async def test_recommendation_staleness(mock_registry):
    """Test time-based staleness filtering."""
    from mcp_server.services.recommendation.config import RecommendationConfig

    now = datetime.now(timezone.utc)
    one_day = timedelta(days=1)

    # max_age = 5 days
    config = RecommendationConfig(
        limit_default=3,
        candidate_multiplier=2,
        fallback_enabled=True,
        fallback_threshold=0.85,
        status_priority=["verified", "seeded"],
        exclude_tombstoned=True,
        stale_max_age_days=5,
        diversity_enabled=False,
        diversity_max_per_source=-1,
        diversity_min_verified=0,
    )

    # 1. Fresh (1 day old)
    fresh = make_qp("fresh", "verified")
    fresh.updated_at = now - one_day

    # 2. Stale (6 days old)
    stale = make_qp("stale", "verified")
    stale.updated_at = now - timedelta(days=6)

    # 3. Missing updated_at (should be excluded if staleness enabled)
    unknown = make_qp("unknown", "verified")
    unknown.updated_at = None

    mock_registry.lookup_semantic.return_value = [fresh, stale, unknown]

    with patch("mcp_server.services.recommendation.service.RECO_CONFIG", config):
        result = await RecommendationService.recommend_examples("test", 3)

        assert len(result.examples) == 1
        assert result.examples[0].canonical_group_id == "fresh"

    # 4. Disable staleness (max_age = 0)
    config_disabled = RecommendationConfig(
        limit_default=3,
        candidate_multiplier=2,
        fallback_enabled=True,
        fallback_threshold=0.85,
        status_priority=["verified", "seeded"],
        exclude_tombstoned=True,
        stale_max_age_days=0,
        diversity_enabled=False,
        diversity_max_per_source=-1,
        diversity_min_verified=0,
    )
    with patch("mcp_server.services.recommendation.service.RECO_CONFIG", config_disabled):
        result = await RecommendationService.recommend_examples("test", 3)
        # Should include stale and unknown now
        # Note: deduping happens, but they have diff fingerprints
        assert len(result.examples) == 3


@pytest.mark.asyncio
async def test_recommendation_filtering_regression(mock_registry):
    """Regression: All primaries filtered, fallback still filtered and returned."""
    # 1. Primaries are all tombstoned
    p1 = make_qp("p1", "verified")
    p1.status = "tombstoned"
    p2 = make_qp("p2", "seeded")
    p2.status = "tombstoned"

    # 2. Fallback has one tombstoned, one valid
    f1 = make_qp("f1", "unverified")
    f1.status = "tombstoned"
    f1.roles = ["interaction"]

    f2 = make_qp("f2", "unverified")
    f2.roles = ["interaction"]

    mock_registry.lookup_semantic.side_effect = [
        [p1],  # verified
        [p2],  # seeded
        [f1, f2],  # fallback
    ]

    result = await RecommendationService.recommend_examples("test", 1, limit=1)

    # Result should only contain f2
    assert len(result.examples) == 1
    assert result.examples[0].canonical_group_id == "f2"
    assert result.fallback_used is True


@pytest.mark.asyncio
async def test_recommendation_diversity_with_fallback(mock_registry):
    """Test diversity policy across primary and fallback sources."""
    # Scenario:
    # - limit = 3
    # - diversity_max_per_source = 1
    # - Primary has two verified (F1, F2)
    # - Fallback has one unverified (F3)

    config = RecommendationConfig(
        limit_default=3,
        candidate_multiplier=2,
        fallback_enabled=True,
        fallback_threshold=0.85,
        status_priority=["verified", "seeded"],
        exclude_tombstoned=True,
        stale_max_age_days=0,
        diversity_enabled=True,
        diversity_max_per_source=1,
        diversity_min_verified=0,
    )

    v1 = make_qp("f1", "verified")
    v2 = make_qp("f2", "verified")
    f3 = make_qp("f3", "unverified")
    f3.roles = ["interaction"]

    # Primaries: [v1, v2]
    # Fallback: [f3]
    mock_registry.lookup_semantic.side_effect = [
        [v1, v2],  # verified
        [],  # seeded
        [f3],  # fallback
    ]

    with patch("mcp_server.services.recommendation.service.RECO_CONFIG", config):
        result = await RecommendationService.recommend_examples("test", 1, limit=3)

        # Expected:
        # - v1 selected (approved count=1)
        # - v2 skipped (approved count=1 >= max 1)
        # - f3 selected (fallback count=1)
        # Note: Even though limit is 3, we only get 2 because of diversity caps.
        assert len(result.examples) == 2
        sources = [ex.source for ex in result.examples]
        assert "approved" in sources
        assert "fallback" in sources
        assert result.fallback_used is True


@pytest.mark.asyncio
async def test_recommendation_diversity_disabled_fallback(mock_registry):
    """Test that fallback behavior is unchanged when diversity is disabled."""
    config = RecommendationConfig(
        limit_default=3,
        candidate_multiplier=2,
        fallback_enabled=True,
        fallback_threshold=0.85,
        status_priority=["verified", "seeded"],
        exclude_tombstoned=True,
        stale_max_age_days=0,
        diversity_enabled=False,
        diversity_max_per_source=1,  # Should be ignored
        diversity_min_verified=0,
    )

    v1 = make_qp("f1", "verified")
    v2 = make_qp("f2", "verified")
    f3 = make_qp("f3", "unverified")
    f3.roles = ["interaction"]

    mock_registry.lookup_semantic.side_effect = [
        [v1, v2],  # verified
        [],  # seeded
        [f3],  # fallback
    ]

    with patch("mcp_server.services.recommendation.service.RECO_CONFIG", config):
        result = await RecommendationService.recommend_examples("test", 1, limit=3)

        # Expected: [v1, v2, f3] as diversity is disabled
        assert len(result.examples) == 3
        assert len(result.examples) == 3
        assert result.fallback_used is True


@pytest.mark.asyncio
async def test_diversity_across_sources_including_fallback(mock_registry, diversity_pool):
    """Test that diversity caps apply across primary and fallback sources."""
    # RECO_DIVERSITY_MAX_PER_SOURCE = 1
    # limit = 3
    # v1, v2 (verified)
    # s1, s2 (seeded)
    # f1, f2 (fallback)
    config = RecommendationConfig(
        limit_default=3,
        candidate_multiplier=2,
        fallback_enabled=True,
        fallback_threshold=0.85,
        status_priority=["verified", "seeded"],
        exclude_tombstoned=True,
        stale_max_age_days=0,
        diversity_enabled=True,
        diversity_max_per_source=1,
        diversity_min_verified=0,
    )

    mock_registry.lookup_semantic.side_effect = [
        [diversity_pool["v1"]],  # Only 1 verified (F1)
        [diversity_pool["s1"]],  # Only 1 seeded (S1)
        [diversity_pool["f1"], diversity_pool["f2"]],  # Fallback
    ]

    with patch("mcp_server.services.recommendation.service.RECO_CONFIG", config):
        result = await RecommendationService.recommend_examples("test", 1, limit=3)

        # Expected: 1 approved, 1 seeded, 1 fallback
        assert len(result.examples) == 3
        sources = [ex.source for ex in result.examples]
        assert sources.count("approved") == 1
        assert sources.count("seeded") == 1
        assert sources.count("fallback") == 1
        assert result.fallback_used is True


@pytest.mark.asyncio
async def test_diversity_verified_priority_preserved(mock_registry, diversity_pool):
    """Test that verified floor and priority are preserved under diversity selection."""
    # RECO_DIVERSITY_MIN_VERIFIED = 1
    # limit = 2
    # s1, s2 (seeded)
    # v1 (verified) - at the end of primary list
    config = RecommendationConfig(
        limit_default=2,
        candidate_multiplier=2,
        fallback_enabled=True,
        fallback_threshold=0.85,
        status_priority=["verified", "seeded"],
        exclude_tombstoned=True,
        stale_max_age_days=0,
        diversity_enabled=True,
        diversity_max_per_source=2,
        diversity_min_verified=1,
    )

    # Note: lookup_semantic returns verified then seeded.
    mock_registry.lookup_semantic.side_effect = [
        [diversity_pool["v1"]],
        [diversity_pool["s1"], diversity_pool["s2"]],
        [],
    ]

    with patch("mcp_server.services.recommendation.service.RECO_CONFIG", config):
        result = await RecommendationService.recommend_examples("test", 1, limit=2)

        assert len(result.examples) == 2
        # First one should be approved due to status priority AND floor logic
        assert result.examples[0].source == "approved"
        assert result.examples[1].source == "seeded"


@pytest.mark.asyncio
async def test_diversity_fingerprint_uniqueness_enforced(mock_registry, diversity_pool):
    """Test that fingerprint uniqueness is never violated across sources."""
    # limit = 3
    # v1 (F1)
    # dup_v1 (F1)
    # f1 (F4)
    config = RecommendationConfig(
        limit_default=3,
        candidate_multiplier=2,
        fallback_enabled=True,
        fallback_threshold=0.85,
        status_priority=["verified", "seeded"],
        exclude_tombstoned=True,
        stale_max_age_days=0,
        diversity_enabled=True,
        diversity_max_per_source=5,  # High enough to not be the bottleneck
        diversity_min_verified=0,
    )

    mock_registry.lookup_semantic.side_effect = [
        [diversity_pool["v1"], diversity_pool["dup_v1"]],
        [],
        [diversity_pool["f1"]],
    ]

    with patch("mcp_server.services.recommendation.service.RECO_CONFIG", config):
        result = await RecommendationService.recommend_examples("test", 1, limit=3)

        # Expected: [v1, f1] (dup_v1 skipped)
        assert len(result.examples) == 2
        fingerprints = [ex.canonical_group_id for ex in result.examples]
        assert len(set(fingerprints)) == len(fingerprints)
        assert "F1" in fingerprints
        assert "F4" in fingerprints
