"""Tests for dataset-aware constraint pattern provider."""

from mcp_server.services.cache.constraint_patterns import get_constraint_patterns


def test_get_constraint_patterns_synthetic(monkeypatch):
    """Test patterns for synthetic mode (default)."""
    monkeypatch.setenv("DATASET_MODE", "synthetic")
    patterns = get_constraint_patterns()

    # Should have no rating patterns
    assert len(patterns.rating_patterns) == 0

    # Should have no film/actor entities,
    # but minimal synthetic entities (merchant, account, transaction, institution)
    entities = [p[1] for p in patterns.entity_patterns]
    assert len(entities) == 4
    assert "actor" not in entities
    assert "rental" not in entities


def test_get_constraint_patterns_explicit_arg():
    """Test overriding mode via argument."""
    # Synthetic override
    patterns = get_constraint_patterns(dataset_mode="synthetic")
    assert len(patterns.rating_patterns) == 0


def test_get_constraint_patterns_default(monkeypatch):
    """Test default behavior when env var is missing (defaults to synthetic)."""
    monkeypatch.delenv("DATASET_MODE", raising=False)
    # common.config.dataset.get_dataset_mode defaults to "synthetic"
    patterns = get_constraint_patterns()
    assert len(patterns.rating_patterns) == 0
    assert len(patterns.entity_patterns) == 4
