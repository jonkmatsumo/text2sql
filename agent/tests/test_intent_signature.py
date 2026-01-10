"""Unit tests for intent signature generation."""

from agent_core.cache.intent_signature import IntentSignature, build_signature_from_constraints


class TestIntentSignature:
    """Tests for IntentSignature class."""

    def test_to_canonical_json_basic(self):
        """Test canonical JSON generation with basic fields."""
        sig = IntentSignature(
            intent="top_actors_by_film_count",
            entity="actor",
            filters={"rating": "PG"},
        )
        json_str = sig.to_canonical_json()

        # Should be sorted, compact, and lowercase (except rating value)
        assert '"entity":"actor"' in json_str
        assert '"intent":"top_actors_by_film_count"' in json_str
        assert '"rating":"PG"' in json_str
        # No spaces
        assert " " not in json_str

    def test_to_canonical_json_stable_ordering(self):
        """Test that canonical JSON has stable key ordering."""
        sig1 = IntentSignature(
            intent="test",
            entity="actor",
            filters={"rating": "G", "year": "2023"},
        )
        sig2 = IntentSignature(
            intent="test",
            entity="actor",
            filters={"year": "2023", "rating": "G"},  # Different order
        )

        # Should produce identical output
        assert sig1.to_canonical_json() == sig2.to_canonical_json()

    def test_compute_key_deterministic(self):
        """Test that compute_key produces deterministic hash."""
        sig1 = IntentSignature(
            intent="top_actors_by_film_count",
            entity="actor",
            filters={"rating": "PG"},
            ranking={"limit": 10},
        )
        sig2 = IntentSignature(
            intent="top_actors_by_film_count",
            entity="actor",
            filters={"rating": "PG"},
            ranking={"limit": 10},
        )

        assert sig1.compute_key() == sig2.compute_key()
        assert len(sig1.compute_key()) == 64  # SHA256 hex

    def test_compute_key_different_for_different_ratings(self):
        """Critical test: PG and G must produce different keys."""
        sig_pg = IntentSignature(
            intent="top_actors_by_film_count",
            entity="actor",
            filters={"rating": "PG"},
        )
        sig_g = IntentSignature(
            intent="top_actors_by_film_count",
            entity="actor",
            filters={"rating": "G"},
        )

        assert sig_pg.compute_key() != sig_g.compute_key()

    def test_compute_key_different_for_all_ratings(self):
        """Test that all ratings produce distinct keys."""
        ratings = ["G", "PG", "PG-13", "R", "NC-17"]
        keys = set()

        for rating in ratings:
            sig = IntentSignature(
                intent="top_actors_by_film_count",
                filters={"rating": rating},
            )
            keys.add(sig.compute_key())

        # All should be unique
        assert len(keys) == len(ratings)

    def test_to_dict_roundtrip(self):
        """Test that to_dict and from_dict are inverses."""
        original = IntentSignature(
            intent="top_actors",
            entity="actor",
            item="film",
            metric="count_distinct",
            filters={"rating": "R"},
            ranking={"limit": 10, "include_ties": True},
        )

        data = original.to_dict()
        restored = IntentSignature.from_dict(data)

        assert original.intent == restored.intent
        assert original.entity == restored.entity
        assert original.filters == restored.filters
        assert original.ranking == restored.ranking


class TestBuildSignatureFromConstraints:
    """Tests for build_signature_from_constraints function."""

    def test_build_with_rating_and_limit(self):
        """Test building signature with rating and limit."""
        sig = build_signature_from_constraints(
            query="Top 10 actors in PG films",
            rating="PG",
            limit=10,
            entity="actor",
        )

        assert sig.filters.get("rating") == "PG"
        assert sig.ranking.get("limit") == 10
        assert sig.entity == "actor"
        assert sig.intent == "top_actors_by_film_count"

    def test_build_with_ties(self):
        """Test building signature with include_ties."""
        sig = build_signature_from_constraints(
            query="Top 10 actors including ties",
            rating="G",
            limit=10,
            include_ties=True,
            entity="actor",
        )

        assert sig.ranking.get("include_ties") is True

    def test_build_infers_intent(self):
        """Test that intent is inferred from query patterns."""
        sig = build_signature_from_constraints(
            query="Top 10 actors in R films",
            rating="R",
            limit=10,
            entity="actor",
        )

        assert sig.intent == "top_actors_by_film_count"
        assert sig.item == "film"

    def test_different_ratings_different_keys(self):
        """Critical test: Same query structure but different ratings."""
        sig_pg = build_signature_from_constraints(
            query="Top 10 actors in PG films",
            rating="PG",
            limit=10,
            entity="actor",
        )
        sig_g = build_signature_from_constraints(
            query="Top 10 actors in G films",
            rating="G",
            limit=10,
            entity="actor",
        )
        sig_r = build_signature_from_constraints(
            query="Top 10 actors in R films",
            rating="R",
            limit=10,
            entity="actor",
        )

        keys = {sig_pg.compute_key(), sig_g.compute_key(), sig_r.compute_key()}
        assert len(keys) == 3  # All must be distinct
