"""Unit tests for query grounding utilities."""

import sys
from unittest.mock import MagicMock, patch


class TestGroundQueryForRetrieval:
    """Tests for ground_query_for_retrieval function."""

    def test_grounding_with_entity_mappings(self):
        """When canonicalizer finds mappings, query should include schema hints."""
        # Create mock service
        mock_service = MagicMock()
        mock_service.is_available.return_value = True

        # Create a mock SpaCy doc with entities
        mock_ent = MagicMock()
        mock_ent.text = "customers"
        mock_ent.ent_id_ = "users"
        mock_ent.label_ = "TABLE"

        mock_doc = MagicMock()
        mock_doc.ents = [mock_ent]

        mock_nlp = MagicMock()
        mock_nlp.return_value = mock_doc
        mock_service.nlp = mock_nlp

        # Create mock module
        mock_canon_module = MagicMock()
        mock_canon_module.CanonicalizationService.get_instance.return_value = mock_service

        with patch.dict(sys.modules, {"mcp_server.services.canonicalization": mock_canon_module}):
            # Re-import to pick up the mock
            from agent_core.utils import grounding

            # Clear any cached module state
            if hasattr(grounding, "_cache"):
                delattr(grounding, "_cache")

            result = grounding.ground_query_for_retrieval("Who are the customers?")

            assert "customers≈users" in result
            assert "Schema hints:" in result

    def test_grounding_no_mappings_returns_original(self):
        """When no entity mappings found, return original query."""
        mock_service = MagicMock()
        mock_service.is_available.return_value = True

        mock_doc = MagicMock()
        mock_doc.ents = []

        mock_nlp = MagicMock()
        mock_nlp.return_value = mock_doc
        mock_service.nlp = mock_nlp

        mock_canon_module = MagicMock()
        mock_canon_module.CanonicalizationService.get_instance.return_value = mock_service

        with patch.dict(sys.modules, {"mcp_server.services.canonicalization": mock_canon_module}):
            from agent_core.utils import grounding

            query = "What is the total revenue?"
            result = grounding.ground_query_for_retrieval(query)

            assert result == query

    def test_grounding_service_unavailable_returns_original(self):
        """When CanonicalizationService is unavailable, return original query."""
        mock_service = MagicMock()
        mock_service.is_available.return_value = False

        mock_canon_module = MagicMock()
        mock_canon_module.CanonicalizationService.get_instance.return_value = mock_service

        with patch.dict(sys.modules, {"mcp_server.services.canonicalization": mock_canon_module}):
            from agent_core.utils import grounding

            query = "Who are the customers?"
            result = grounding.ground_query_for_retrieval(query)

            assert result == query

    def test_grounding_import_error_returns_original(self):
        """When CanonicalizationService cannot be imported, return original query."""
        # Remove mock and let ImportError happen
        modules_backup = sys.modules.copy()

        # Remove any mcp_server modules to simulate ImportError
        for key in list(sys.modules.keys()):
            if key.startswith("mcp_server"):
                del sys.modules[key]

        try:
            from agent_core.utils import grounding

            query = "Who are the customers?"
            result = grounding.ground_query_for_retrieval(query)

            # Should gracefully fall back to original query
            assert result == query
        finally:
            # Restore modules
            sys.modules.update(modules_backup)


class TestExtractSchemaHints:
    """Tests for extract_schema_hints function."""

    def test_multiple_entities_returns_all_mappings(self):
        """Multiple entities should all be included in mappings."""
        mock_service = MagicMock()
        mock_service.is_available.return_value = True

        # Create mock entities
        mock_ent1 = MagicMock()
        mock_ent1.text = "customers"
        mock_ent1.ent_id_ = "users"
        mock_ent1.label_ = "TABLE"

        mock_ent2 = MagicMock()
        mock_ent2.text = "purchases"
        mock_ent2.ent_id_ = "orders"
        mock_ent2.label_ = "TABLE"

        mock_doc = MagicMock()
        mock_doc.ents = [mock_ent1, mock_ent2]

        mock_nlp = MagicMock()
        mock_nlp.return_value = mock_doc
        mock_service.nlp = mock_nlp

        mock_canon_module = MagicMock()
        mock_canon_module.CanonicalizationService.get_instance.return_value = mock_service

        with patch.dict(sys.modules, {"mcp_server.services.canonicalization": mock_canon_module}):
            from agent_core.utils import grounding

            grounded, mappings = grounding.extract_schema_hints("Show customers and purchases")

            assert len(mappings) == 2
            assert any(m["canonical_id"] == "users" for m in mappings)
            assert any(m["canonical_id"] == "orders" for m in mappings)
            assert "customers≈users" in grounded
            assert "purchases≈orders" in grounded
