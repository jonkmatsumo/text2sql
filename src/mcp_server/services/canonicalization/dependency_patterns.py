"""Dependency patterns for structural constraint extraction.

These patterns use SpaCy's DependencyMatcher to identify constraints
based on grammatical relationships, not string adjacency.

Pattern sets:
- LIMIT_PATTERNS: Domain-agnostic (work for all datasets)
- ENTITY_PATTERNS: Financial patterns

For custom domains, provide patterns via PATTERNS_DIR env var
pointing to a directory containing domain-specific .jsonl pattern files.
"""

from typing import List

# Pattern: "top 10", "first 5", "best 20"
# Uses sibling operator to handle adjacent ordinal + number
LIMIT_PATTERN = [
    {
        "RIGHT_ID": "ordinal",
        "RIGHT_ATTRS": {"LEMMA": {"IN": ["top", "first", "best", "bottom", "last"]}},
    },
    {
        "LEFT_ID": "ordinal",
        "REL_OP": ".",  # Immediate sibling
        "RIGHT_ID": "count",
        "RIGHT_ATTRS": {"POS": "NUM"},
    },
]

# Alternative: "10 top movies" (number precedes ordinal)
LIMIT_PATTERN_ALT = [
    {"RIGHT_ID": "count", "RIGHT_ATTRS": {"POS": "NUM"}},
    {
        "LEFT_ID": "count",
        "REL_OP": ".",
        "RIGHT_ID": "ordinal",
        "RIGHT_ATTRS": {"LEMMA": {"IN": ["top", "best"]}},
    },
]

LIMIT_PATTERNS = [LIMIT_PATTERN, LIMIT_PATTERN_ALT]

# Pattern: "4 star restaurants", "5-star hotels"
NUMERIC_RATING_PATTERN = [
    {"RIGHT_ID": "unit", "RIGHT_ATTRS": {"LEMMA": {"IN": ["star", "rating", "point"]}}},
    {
        "LEFT_ID": "unit",
        "REL_OP": ">",  # unit governs count
        "RIGHT_ID": "count",
        "RIGHT_ATTRS": {"POS": "NUM"},
    },
]

ENTITY_PATTERN_FINANCIAL = [
    {
        "RIGHT_ID": "entity",
        "RIGHT_ATTRS": {
            "LEMMA": {
                "IN": [
                    "merchant",
                    "transaction",
                    "account",
                    "institution",
                    "bank",
                    "customer",
                    "payment",
                ]
            }
        },
    }
]

ENTITY_PATTERNS = [ENTITY_PATTERN_FINANCIAL]


# ============================================================================
# Dataset-Mode Aware Pattern Getters
# ============================================================================


def get_rating_patterns() -> List:
    """Get rating patterns.

    Returns:
        Empty list (financial domain has no film ratings).
    """
    return []


def get_entity_patterns() -> List:
    """Get entity patterns for financial domain.

    Returns:
        Financial entity patterns.
    """
    # Synthetic mode: only financial entities
    return [ENTITY_PATTERN_FINANCIAL]


def get_all_patterns() -> dict:
    """Get all patterns.

    Returns:
        Dict with 'rating', 'limit', 'entity' pattern lists.
    """
    return {
        "rating": get_rating_patterns(),
        "limit": LIMIT_PATTERNS,  # Domain-agnostic
        "entity": get_entity_patterns(),
    }
