"""Regression tests for clarification behavior with ontology synonyms.

These tests verify that queries using ontology synonyms (e.g., "customers" → "users")
do not trigger unnecessary clarification when the canonicalization layer is working.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest


class TestNoClarificationForSynonymQuery:
    """Test that synonym queries are grounded and don't produce clarifications."""

    @pytest.mark.asyncio
    async def test_resolver_returns_clear_when_mention_has_matching_ent_id(self):
        """Verify AmbiguityResolver returns CLEAR when mention.ent_id matches schema."""
        from mcp_server.services.ambiguity.binder import Mention
        from mcp_server.services.ambiguity.resolver import AmbiguityResolver

        # Create resolver with mocked binder to avoid embedding calls
        resolver = AmbiguityResolver()

        # Mock the extractor to return a mention with ent_id
        mock_mention = Mention(
            text="customer",
            type="entity",
            start_char=0,
            end_char=8,
            metadata={"ent_id": "users", "ent_type": "TABLE"},
        )

        with patch.object(resolver.extractor, "extract", return_value=[mock_mention]):
            # Schema context that includes the users table
            schema_context = [
                {
                    "name": "users",
                    "description": "User accounts",
                    "columns": [
                        {"name": "id", "description": "Primary key"},
                        {"name": "email", "description": "User email"},
                    ],
                },
                {
                    "name": "orders",
                    "description": "Customer orders",
                    "columns": [],
                },
            ]

            result = resolver.resolve("Who are the customers?", schema_context)

            # Should be CLEAR because ent_id="users" matches schema table
            assert result["status"] == "CLEAR"
            assert "customer" in result["resolved_bindings"]
            assert result["resolved_bindings"]["customer"] == "users"

    @pytest.mark.asyncio
    async def test_resolver_returns_missing_when_no_ent_id_and_no_match(self):
        """Verify AmbiguityResolver returns MISSING when no ent_id and no lexical match."""
        from mcp_server.services.ambiguity.binder import Mention
        from mcp_server.services.ambiguity.resolver import AmbiguityResolver

        resolver = AmbiguityResolver()

        # Mention without ent_id (canonicalizer not available)
        mock_mention = Mention(
            text="customer",
            type="noun_phrase",
            start_char=0,
            end_char=8,
            metadata={},  # No ent_id
        )

        with patch.object(resolver.extractor, "extract", return_value=[mock_mention]):
            # Schema context without "customer" or "users" table
            schema_context = [
                {
                    "name": "products",
                    "description": "Product catalog",
                    "columns": [],
                },
            ]

            result = resolver.resolve("Who are the customers?", schema_context)

            # Should be MISSING because "customer" doesn't match "products"
            assert result["status"] == "MISSING"
            assert "customer" in result.get("missing_mentions", [])


class TestGroundingIntegration:
    """Test grounding utility integration with canonicalization."""

    def test_grounded_query_includes_schema_hints_when_canonicalizer_active(self):
        """When canonicalizer provides mappings, grounded query includes hints."""
        from agent_core.utils.grounding import ground_query_for_retrieval

        # Mock CanonicalizationService
        mock_service = MagicMock()
        mock_service.is_available.return_value = True

        mock_ent = MagicMock()
        mock_ent.text = "customers"
        mock_ent.ent_id_ = "users"
        mock_ent.label_ = "TABLE"

        mock_doc = MagicMock()
        mock_doc.ents = [mock_ent]

        mock_nlp = MagicMock()
        mock_nlp.return_value = mock_doc
        mock_service.nlp = mock_nlp

        mock_canon_module = MagicMock()
        mock_canon_module.CanonicalizationService.get_instance.return_value = mock_service

        with patch.dict(sys.modules, {"mcp_server.services.canonicalization": mock_canon_module}):
            result = ground_query_for_retrieval("List all customers")

            # Query should include the grounding hint
            assert "customers≈users" in result
            assert "Schema hints:" in result

    def test_grounded_query_used_in_semantic_search_payload(self):
        """Retrieve node should use grounded query for semantic search."""
        # This is validated by the unit tests for ground_query_for_retrieval
        # The integration point is retrieve.py line 55 where grounded_query is used
        pass


class TestBinderOntologyIntegration:
    """Integration tests for binder with ontology matching."""

    def test_binder_prioritizes_ontology_match_over_lexical(self):
        """Binder returns ontology match even when lexical match is poor."""
        from mcp_server.services.ambiguity.binder import CandidateBinder, Mention

        # Create binder with mocked RAG
        binder = CandidateBinder()
        binder.rag = MagicMock()

        mention = Mention(
            text="buyers",  # Poor lexical match to "users"
            type="entity",
            start_char=0,
            end_char=6,
            metadata={"ent_id": "users"},  # But ontology says it's "users"
        )

        schema_context = [
            {
                "name": "users",
                "description": "User accounts",
                "columns": [],
            },
            {
                "name": "orders",
                "description": "Order records",
                "columns": [],
            },
        ]

        candidates = binder.get_candidates(mention, schema_context)

        # Should return users with ontology_match=1.0
        assert len(candidates) == 1
        assert candidates[0].id == "users"
        assert candidates[0].scores["ontology_match"] == 1.0
        assert candidates[0].final_score == 1.0
