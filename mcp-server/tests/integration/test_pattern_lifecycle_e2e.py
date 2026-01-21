import os
import uuid
from unittest.mock import patch

import pytest
import spacy

from dal.database import Database
from mcp_server.services.canonicalization.spacy_pipeline import CanonicalizationService

# Ensure SpaCy is enabled for these tests
os.environ["SPACY_ENABLED"] = "true"

pytestmark = pytest.mark.requires_db


@pytest.fixture
def reset_canonicalization_service():
    """Reset singleton before and after test."""
    CanonicalizationService.reset_instance()
    yield
    CanonicalizationService.reset_instance()


@pytest.fixture
def mock_spacy_pipeline():
    """Mock spacy.load to return a blank model, avoiding downloads."""
    # Use a unique factory name to avoid collision with built-in 'ner'
    if "test_ner" not in spacy.registry.factories:

        @spacy.Language.component("test_ner")
        def test_ner_fn(doc):
            return doc

    def create_model(*args, **kwargs):
        blank_nlp = spacy.blank("en")
        if "ner" not in blank_nlp.pipe_names:
            blank_nlp.add_pipe("test_ner", name="ner")
        return blank_nlp

    # We need to patch spacy.load to return a NEW blank model each time
    with patch("spacy.load", side_effect=create_model) as mock_load:
        yield mock_load


@pytest.mark.asyncio
@pytest.mark.integration
async def test_pattern_lifecycle_scaffold(reset_canonicalization_service, mock_spacy_pipeline):
    """Phase 1: Verify we can insert a pattern and reload it."""
    # 1. Setup Data
    unique_pattern = f"TEST_PATTERN_{uuid.uuid4().hex[:8]}"
    label = "RATING"

    # Insert directly via Database connection
    async with Database.get_connection() as conn:
        await conn.execute(
            """
            INSERT INTO nlp_patterns (id, label, pattern)
            VALUES ($1, $2, $3)
            """,
            str(uuid.uuid4()),
            label,
            unique_pattern,
        )

    # 2. Initialize Service & Reload
    # Note: reset_canonicalization_service ensures fresh start
    service = CanonicalizationService.get_instance()

    # Debug: Check if row exists
    async with Database.get_connection() as conn:
        rows = await conn.fetch("SELECT * FROM nlp_patterns")
        print(f"DEBUG: Found {len(rows)} rows in nlp_patterns: {rows}")

    # Reload patterns with patched env var to ensure it's enabled
    with patch("mcp_server.services.canonicalization.spacy_pipeline.SPACY_ENABLED", True):
        # This should query DB, find our new pattern, and update the pipeline
        count = await service.reload_patterns()

    # 3. Assertions
    # We expect at least 1 pattern (ours)
    assert count >= 1

    # Verify internal state has the pattern roughly
    # (Since we mocked spacy.load with blank("en"), the entity ruler should be added)
    assert service._state is not None
    assert service._state.nlp is not None
    assert "entity_ruler" in service._state.nlp.pipe_names


@pytest.mark.asyncio
@pytest.mark.integration
async def test_pattern_resolution_end_to_end(reset_canonicalization_service, mock_spacy_pipeline):
    """Phase 2: Verify inserted patterns are resolved in queries."""
    # 1. Insert Pattern for Resolution
    # Use a pattern that doesn't conflict with anything
    unique_val = f"MY_UNIQUE_RATING_{uuid.uuid4().hex[:4]}"
    canonical_id = str(uuid.uuid4())
    label = "RATING"

    async with Database.get_connection() as conn:
        await conn.execute(
            """
            INSERT INTO nlp_patterns (id, label, pattern)
            VALUES ($1, $2, $3)
            """,
            canonical_id,
            label,
            unique_val,
        )

    service = CanonicalizationService.get_instance()

    # 2. Reload with Mocked DependencyMatcher
    # We mock _setup_dependency_matcher because blank("en") doesn't support
    # dependency parsing required by the matcher. We only test EntityRuler here.
    with patch.object(
        CanonicalizationService, "_setup_dependency_matcher", return_value=None
    ), patch("mcp_server.services.canonicalization.spacy_pipeline.SPACY_ENABLED", True):
        await service.reload_patterns()

        # 3. Run Query

        query = f"Show me movies rated {unique_val}"
        constraints = service.extract_constraints(query)

        # 4. Assert
        # Should resolve to the canonical ID we inserted
        assert constraints["rating"] == canonical_id
        assert constraints["confidence"] > 0
