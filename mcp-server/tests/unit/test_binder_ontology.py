"""Unit tests for CandidateBinder ontology match behavior.

Tests the fix for clarification regression where ent_id from SpaCy
EntityRuler should short-circuit candidate scoring.
"""

import pytest
from mcp_server.services.ambiguity.binder import Candidate, CandidateBinder, Mention


class TestCandidateOntologyMatch:
    """Tests for Candidate.final_score with ontology_match."""

    def test_ontology_match_short_circuits_final_score(self):
        """When ontology_match=1.0, final_score should be 1.0 regardless of other scores."""
        candidate = Candidate(
            kind="table",
            id="users",
            label="Users table",
            scores={"ontology_match": 1.0, "lexical": 0.2, "semantic": 0.1},
            metadata={},
        )
        assert candidate.final_score == 1.0

    def test_ontology_match_missing_uses_weighted_score(self):
        """Without ontology_match, final_score uses weighted average."""
        candidate = Candidate(
            kind="table",
            id="users",
            label="Users table",
            scores={"lexical": 0.8, "semantic": 0.6, "relational": 1.0},
            metadata={},
        )
        # Expected: 0.8*0.5 + 0.6*0.3 + 1.0*0.2 = 0.4 + 0.18 + 0.2 = 0.78
        assert abs(candidate.final_score - 0.78) < 0.01


class TestCandidateBinderOntologyPrecedence:
    """Tests for CandidateBinder.get_candidates with ontology ent_id."""

    @pytest.fixture
    def binder(self, mocker):
        """Create a binder with mocked RagEngine to avoid embedding calls."""
        mocker.patch(
            "mcp_server.services.ambiguity.binder.CandidateBinder.__init__",
            lambda self: setattr(self, "rag", None),
        )
        return CandidateBinder()

    @pytest.fixture
    def sample_schema_context(self):
        """Sample schema context with users table and columns."""
        return [
            {
                "name": "users",
                "description": "User accounts",
                "columns": [
                    {"name": "id", "description": "Primary key"},
                    {"name": "email", "description": "User email address"},
                ],
            },
            {
                "name": "orders",
                "description": "Customer orders",
                "columns": [
                    {"name": "id", "description": "Order ID"},
                    {"name": "user_id", "description": "Foreign key to users"},
                ],
            },
        ]

    def test_ent_id_matches_table_returns_single_high_confidence_candidate(
        self, binder, sample_schema_context
    ):
        """When ent_id matches a table name, return that table with ontology_match=1.0."""
        mention = Mention(
            text="customer",  # Surface text doesn't match
            type="entity",
            start_char=0,
            end_char=8,
            metadata={"ent_id": "users"},  # Canonical ID from ontology
        )

        candidates = binder.get_candidates(mention, sample_schema_context)

        assert len(candidates) == 1
        assert candidates[0].id == "users"
        assert candidates[0].kind == "table"
        assert candidates[0].scores.get("ontology_match") == 1.0
        assert candidates[0].final_score == 1.0
        assert candidates[0].metadata.get("matched_via") == "ontology"

    def test_ent_id_matches_column_returns_single_high_confidence_candidate(
        self, binder, sample_schema_context
    ):
        """When ent_id matches a column ID, return that column with ontology_match=1.0."""
        mention = Mention(
            text="customer email",
            type="entity",
            start_char=0,
            end_char=14,
            metadata={"ent_id": "users.email"},  # Column canonical ID
        )

        candidates = binder.get_candidates(mention, sample_schema_context)

        assert len(candidates) == 1
        assert candidates[0].id == "users.email"
        assert candidates[0].kind == "column"
        assert candidates[0].scores.get("ontology_match") == 1.0
        assert candidates[0].final_score == 1.0

    def test_ent_id_no_match_falls_through_to_lexical_scoring(
        self, binder, sample_schema_context, mocker
    ):
        """When ent_id is present but doesn't match, fall through to normal scoring."""
        # Mock embed_text to avoid actual embedding calls
        mock_embed = mocker.patch.object(binder, "rag", autospec=True)
        mock_embed.embed_text.return_value = [0.0] * 384  # Dummy embedding

        mention = Mention(
            text="products",
            type="entity",
            start_char=0,
            end_char=8,
            metadata={"ent_id": "nonexistent_table"},  # No match
        )

        candidates = binder.get_candidates(mention, sample_schema_context)

        # Should fall through and return multiple candidates from lexical scoring
        assert len(candidates) >= 1
        # None should have ontology_match=1.0
        for c in candidates:
            assert c.scores.get("ontology_match") != 1.0

    def test_no_ent_id_uses_normal_scoring(self, binder, sample_schema_context, mocker):
        """When no ent_id, use standard lexical/semantic scoring."""
        # Mock embed_text to avoid actual embedding calls
        mock_embed = mocker.patch.object(binder, "rag", autospec=True)
        mock_embed.embed_text.return_value = [0.0] * 384

        mention = Mention(
            text="orders",
            type="noun_phrase",
            start_char=0,
            end_char=6,
            metadata={},  # No ent_id
        )

        candidates = binder.get_candidates(mention, sample_schema_context)

        # Should find orders table with good lexical score
        assert len(candidates) >= 1
        # First should be orders (exact match)
        top = candidates[0]
        assert "orders" in top.id.lower()
