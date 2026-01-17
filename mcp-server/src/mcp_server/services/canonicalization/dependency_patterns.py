"""Dependency patterns for structural constraint extraction.

These patterns use SpaCy's DependencyMatcher to identify constraints
based on grammatical relationships, not string adjacency.

NOTE: These default patterns are tuned for a FILM/MOVIE domain schema
(e.g., Pagila dataset). They match terms like "movie", "film", "actor",
and film rating codes (G, PG, PG-13, R, NC-17).

For other domains, provide custom patterns via PATTERNS_DIR env var
pointing to a directory containing domain-specific .jsonl pattern files.
"""

# Pattern 1a: Adjectival modification - Entity based
RATING_AMOD_PATTERN_ENT = [
    {"RIGHT_ID": "target", "RIGHT_ATTRS": {"LEMMA": {"IN": ["movie", "film", "content", "show"]}}},
    {
        "LEFT_ID": "target",
        "REL_OP": "<",
        "RIGHT_ID": "rating",
        "RIGHT_ATTRS": {"ENT_TYPE": "RATING"},
    },
]

# Pattern 1b: Adjectival modification - Literal based
RATING_AMOD_PATTERN_LIT = [
    {"RIGHT_ID": "target", "RIGHT_ATTRS": {"LEMMA": {"IN": ["movie", "film", "content", "show"]}}},
    {
        "LEFT_ID": "target",
        "REL_OP": "<",
        "RIGHT_ID": "rating",
        "RIGHT_ATTRS": {"TEXT": {"IN": ["G", "PG", "PG-13", "R", "NC-17", "NC17", "g", "pg", "r"]}},
    },
]

# Pattern 2a: Explicit predicate - Entity based
RATING_EXPLICIT_PATTERN_ENT = [
    {"RIGHT_ID": "target", "RIGHT_ATTRS": {"LEMMA": {"IN": ["movie", "film"]}}},
    {
        "LEFT_ID": "target",
        "REL_OP": ">",
        "RIGHT_ID": "modifier",
        "RIGHT_ATTRS": {"LEMMA": {"IN": ["rate", "rating", "rated"]}},
    },
    {
        "LEFT_ID": "modifier",
        "REL_OP": ">>",
        "RIGHT_ID": "value",
        "RIGHT_ATTRS": {"ENT_TYPE": "RATING"},
    },
]

# Pattern 2b: Explicit predicate - Literal based
RATING_EXPLICIT_PATTERN_LIT = [
    {"RIGHT_ID": "target", "RIGHT_ATTRS": {"LEMMA": {"IN": ["movie", "film"]}}},
    {
        "LEFT_ID": "target",
        "REL_OP": ">",
        "RIGHT_ID": "modifier",
        "RIGHT_ATTRS": {"LEMMA": {"IN": ["rate", "rating", "rated"]}},
    },
    {
        "LEFT_ID": "modifier",
        "REL_OP": ">>",
        "RIGHT_ID": "value",
        "RIGHT_ATTRS": {"TEXT": {"IN": ["G", "PG", "PG-13", "R", "NC-17", "NC17", "g", "pg", "r"]}},
    },
]

# Combined rating patterns
RATING_PATTERNS = [
    RATING_AMOD_PATTERN_ENT,
    RATING_AMOD_PATTERN_LIT,
    RATING_EXPLICIT_PATTERN_ENT,
    RATING_EXPLICIT_PATTERN_LIT,
]

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

# Entity detection patterns for film/actor
ENTITY_PATTERN_FILM = [
    {"RIGHT_ID": "entity", "RIGHT_ATTRS": {"LEMMA": {"IN": ["movie", "film", "show", "video"]}}}
]

ENTITY_PATTERN_ACTOR = [
    {
        "RIGHT_ID": "entity",
        "RIGHT_ATTRS": {"LEMMA": {"IN": ["actor", "actress", "performer", "star"]}},
    }
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

ENTITY_PATTERNS = [ENTITY_PATTERN_FILM, ENTITY_PATTERN_ACTOR, ENTITY_PATTERN_FINANCIAL]
