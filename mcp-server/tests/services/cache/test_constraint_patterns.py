"""Tests for dataset-aware constraint pattern provider."""

from mcp_server.services.cache.constraint_patterns import get_constraint_patterns


def test_get_constraint_patterns_synthetic(monkeypatch):
    """Test patterns for synthetic mode (default)."""
    monkeypatch.setenv("DATASET_MODE", "synthetic")
    patterns = get_constraint_patterns()

    # Should have no rating patterns
    assert len(patterns.rating_patterns) == 0

    # Should have no film/actor entities, and currently NO synthetic entities (hardcoding removed)
    entities = [p[1] for p in patterns.entity_patterns]
    assert len(entities) == 0
    assert "actor" not in entities
    assert "rental" not in entities


def test_get_constraint_patterns_pagila(monkeypatch):
    """Test patterns for pagila mode."""
    monkeypatch.setenv("DATASET_MODE", "pagila")
    patterns = get_constraint_patterns()

    # Should have rating patterns
    assert len(patterns.rating_patterns) > 0
    ratings = [p[1] for p in patterns.rating_patterns]
    assert "NC-17" in ratings
    assert "PG-13" in ratings

    # Should have film/actor entities
    entities = [p[1] for p in patterns.entity_patterns]
    assert "film" in entities
    assert "actor" in entities
    assert "rental" in entities


def test_get_constraint_patterns_explicit_arg():
    """Test overriding mode via argument."""
    # Synthetic override
    patterns = get_constraint_patterns(dataset_mode="synthetic")
    assert len(patterns.rating_patterns) == 0

    # Pagila override
    patterns = get_constraint_patterns(dataset_mode="pagila")
    assert len(patterns.rating_patterns) > 0


def test_get_constraint_patterns_default(monkeypatch):
    """Test default behavior when env var is missing (defaults to synthetic)."""
    monkeypatch.delenv("DATASET_MODE", raising=False)
    # common.config.dataset.get_dataset_mode defaults to "synthetic"
    patterns = get_constraint_patterns()
    assert len(patterns.rating_patterns) == 0
    assert len(patterns.entity_patterns) == 0
