from mcp_server.models import QueryPair
from mcp_server.services.recommendation.config import RecommendationConfig
from mcp_server.services.recommendation.service import RecommendationService


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
        "safety_enabled": True,
        "safety_max_pattern_length": 100,
        "safety_blocklist_regex": None,
        "safety_require_sanitizable": True,
    }
    defaults.update(kwargs)
    return RecommendationConfig(**defaults)


def make_qp(question, fingerprint="F1", status="verified"):
    """Create a QueryPair."""
    return QueryPair(
        signature_key=f"sig_{fingerprint}",
        tenant_id=1,
        fingerprint=fingerprint,
        question=question,
        sql_query="SELECT 1",
        status=status,
        roles=["example"],
    )


def test_safety_filtering_disabled():
    """Test that safety filtering is ignored when disabled."""
    config = create_config(safety_enabled=False, safety_max_pattern_length=5)
    candidates = [make_qp("Long question")]

    filtered = RecommendationService._filter_invalid_candidates(candidates, config)
    assert len(filtered) == 1


def test_safety_filtering_max_length():
    """Test safety filtering by max pattern length."""
    config = create_config(safety_enabled=True, safety_max_pattern_length=10)
    candidates = [
        make_qp("Short", "F1"),
        make_qp("Very long question that exceeds limit", "F2"),
    ]

    from mcp_server.services.recommendation.explanation import FilteringExplanation

    explanation = FilteringExplanation()

    filtered = RecommendationService._filter_invalid_candidates(candidates, config, explanation)
    assert len(filtered) == 1
    assert filtered[0].fingerprint == "F1"
    assert explanation.safety_removed == 1


def test_safety_filtering_blocklist():
    """Test safety filtering via regex blocklist."""
    config = create_config(safety_enabled=True, safety_blocklist_regex="DROP TABLE|DELETE FROM")
    candidates = [
        make_qp("How to drop table users", "F1"),
        make_qp("Filter by user", "F2"),
    ]

    from mcp_server.services.recommendation.explanation import FilteringExplanation

    explanation = FilteringExplanation()

    filtered = RecommendationService._filter_invalid_candidates(candidates, config, explanation)
    assert len(filtered) == 1
    assert filtered[0].fingerprint == "F2"
    assert explanation.safety_removed == 1


def test_safety_filtering_sanitizable():
    """Test safety filtering via text sanitizer (e.g. regex meta chars)."""
    config = create_config(safety_enabled=True, safety_require_sanitizable=True)
    candidates = [
        make_qp("Safe question", "F1"),
        make_qp("Unsafe question with meta *", "F2"),
    ]

    from mcp_server.services.recommendation.explanation import FilteringExplanation

    explanation = FilteringExplanation()

    filtered = RecommendationService._filter_invalid_candidates(candidates, config, explanation)
    assert len(filtered) == 1
    assert filtered[0].fingerprint == "F1"
    assert explanation.safety_removed == 1
