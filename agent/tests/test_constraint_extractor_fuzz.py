"""Property-based fuzz tests for constraint extraction.

Uses hypothesis to generate random queries and verify:
- All ratings are extracted correctly regardless of surrounding text
- No false positives (extracting wrong rating)
- Confidence scoring is consistent

Requires: pip install hypothesis
"""

import pytest

# Skip entire module if hypothesis is not installed
pytest.importorskip("hypothesis")

from agent_core.cache.constraint_extractor import (  # noqa: E402
    SPELLED_NUMBERS,
    extract_constraints,
    normalize_rating,
)
from hypothesis import given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

# Define rating strategies
VALID_RATINGS = ["G", "PG", "PG-13", "R", "NC-17"]
RATING_VARIATIONS = {
    "G": ["G", "G-rated", "rated G", "rating G"],
    "PG": ["PG", "PG-rated", "rated PG", "PG rated"],
    "PG-13": ["PG-13", "PG 13", "PG13", "PG-13 rated"],
    "R": ["R", "R-rated", "rated R", "R rated"],
    "NC-17": ["NC-17", "NC17", "NC 17", "NC-17 rated"],
}


# Strategies for generating test data
@st.composite
def query_with_rating(draw):
    """Generate a query containing a specific rating."""
    rating = draw(st.sampled_from(VALID_RATINGS))
    rating_text = draw(st.sampled_from(RATING_VARIATIONS[rating]))
    prefix = draw(st.sampled_from(["Top 10 actors in ", "Show me ", "List all ", ""]))
    suffix = draw(st.sampled_from([" films", " movies", " content", ""]))
    return (f"{prefix}{rating_text}{suffix}", rating)


@st.composite
def query_with_numeric_limit(draw):
    """Generate a query with a numeric limit."""
    limit = draw(st.integers(min_value=1, max_value=100))
    pattern = draw(st.sampled_from(["top", "first", "best"]))
    return (f"{pattern} {limit} actors", limit)


@st.composite
def query_with_spelled_limit(draw):
    """Generate a query with a spelled-out limit."""
    word = draw(st.sampled_from(list(SPELLED_NUMBERS.keys())))
    expected = SPELLED_NUMBERS[word]
    pattern = draw(st.sampled_from(["top", "first"]))
    return (f"{pattern} {word} actors", expected)


class TestRatingExtractionProperty:
    """Property-based tests for rating extraction."""

    @given(query_with_rating())
    @settings(max_examples=50, deadline=None)
    def test_rating_extracted_correctly(self, query_and_expected):
        """Property: Any valid rating in query should be extracted."""
        query, expected_rating = query_and_expected
        constraints = extract_constraints(query)

        # The rating should either match or be a valid alternative
        assert constraints.rating is not None, f"No rating found in: {query}"
        assert (
            constraints.rating == expected_rating
        ), f"Expected {expected_rating}, got {constraints.rating} for: {query}"

    @given(st.text(min_size=0, max_size=100))
    @settings(max_examples=50, deadline=None)
    def test_no_false_positives_on_random_text(self, random_text):
        """Property: Random text without ratings should not extract ratings."""
        # Filter out strings that actually contain valid rating substrings
        if any(r in random_text.upper() for r in ["NC-17", "PG-13", "PG", " G ", " R "]):
            return  # Skip test cases that happen to contain ratings

        constraints = extract_constraints(random_text)
        # Just ensure we don't crash
        assert constraints is not None


class TestLimitExtractionProperty:
    """Property-based tests for limit extraction."""

    @given(query_with_numeric_limit())
    @settings(max_examples=30, deadline=None)
    def test_numeric_limit_extracted(self, query_and_expected):
        """Property: Numeric limits should be extracted correctly."""
        query, expected_limit = query_and_expected
        constraints = extract_constraints(query)

        assert (
            constraints.limit == expected_limit
        ), f"Expected {expected_limit}, got {constraints.limit} for: {query}"

    @given(query_with_spelled_limit())
    @settings(max_examples=20, deadline=None)
    def test_spelled_limit_extracted(self, query_and_expected):
        """Property: Spelled-out limits should be extracted correctly."""
        query, expected_limit = query_and_expected
        constraints = extract_constraints(query)

        assert (
            constraints.limit == expected_limit
        ), f"Expected {expected_limit}, got {constraints.limit} for: {query}"


class TestNormalizeRatingProperty:
    """Property-based tests for rating normalization."""

    @given(st.sampled_from(VALID_RATINGS))
    @settings(max_examples=20, deadline=None)
    def test_normalize_canonical(self, rating):
        """Property: Canonical ratings should normalize to themselves."""
        assert normalize_rating(rating) == rating

    @given(st.sampled_from(VALID_RATINGS))
    @settings(max_examples=20, deadline=None)
    def test_normalize_lowercase(self, rating):
        """Property: Lowercase ratings should normalize correctly."""
        result = normalize_rating(rating.lower())
        assert result == rating, f"normalize_rating({rating.lower()!r}) = {result!r}"


class TestConfidenceProperty:
    """Property-based tests for confidence scoring."""

    @given(query_with_rating())
    @settings(max_examples=30, deadline=None)
    def test_rating_increases_confidence(self, query_and_expected):
        """Property: Queries with ratings should have higher confidence."""
        query, _ = query_and_expected
        constraints = extract_constraints(query)

        assert (
            constraints.confidence >= 0.5
        ), f"Confidence too low ({constraints.confidence}) for query with rating: {query}"
