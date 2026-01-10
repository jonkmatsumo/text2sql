"""Dependency patterns for structural constraint extraction.

These patterns use SpaCy's DependencyMatcher to identify constraints
based on grammatical relationships, not string adjacency.
"""

# Pattern 1: Adjectival modification - "PG movies", "R-rated films"
# Matches when a RATING entity modifies a film/movie noun
RATING_AMOD_PATTERN = [
    {"RIGHT_ID": "target", "RIGHT_ATTRS": {"LEMMA": {"IN": ["movie", "film", "content", "show"]}}},
    {
        "LEFT_ID": "target",
        "REL_OP": "<",  # target is governed by rating
        "RIGHT_ID": "rating",
        "RIGHT_ATTRS": {"ENT_TYPE": "RATING"},
    },
]

# Pattern 2: Explicit predicate - "movies rated PG", "films with rating R"
RATING_EXPLICIT_PATTERN = [
    {"RIGHT_ID": "target", "RIGHT_ATTRS": {"LEMMA": {"IN": ["movie", "film"]}}},
    {
        "LEFT_ID": "target",
        "REL_OP": ">",  # target governs modifier
        "RIGHT_ID": "modifier",
        "RIGHT_ATTRS": {"LEMMA": {"IN": ["rate", "rating", "rated"]}},
    },
    {
        "LEFT_ID": "modifier",
        "REL_OP": ">>",  # modifier is ancestor of value (handles intervening words)
        "RIGHT_ID": "value",
        "RIGHT_ATTRS": {"ENT_TYPE": "RATING"},
    },
]

# Combined rating patterns
RATING_PATTERNS = [RATING_AMOD_PATTERN, RATING_EXPLICIT_PATTERN]

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

ENTITY_PATTERNS = [ENTITY_PATTERN_FILM, ENTITY_PATTERN_ACTOR]
