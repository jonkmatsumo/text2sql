"""Constraint extraction from natural language queries.

This module extracts hard constraints (rating, limit, ties, entity, metric)
from user queries using deterministic regex patterns. These constraints are
used to validate cached SQL before serving.
"""

import re
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class QueryConstraints:
    """Extracted constraints from a natural language query."""

    rating: Optional[str] = None  # G, PG, PG-13, R, NC-17
    limit: Optional[int] = None  # e.g., 10
    include_ties: bool = False
    entity: Optional[str] = None  # actor, film, customer, etc.
    metric: Optional[str] = None  # count_distinct, sum, avg, etc.
    confidence: float = 1.0  # Extraction confidence (0.0-1.0)
    _matched_patterns: list = field(default_factory=list)

    def to_dict(self) -> dict:
        """Convert to dictionary for logging/serialization."""
        return {
            "rating": self.rating,
            "limit": self.limit,
            "include_ties": self.include_ties,
            "entity": self.entity,
            "metric": self.metric,
            "confidence": self.confidence,
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        import json

        return json.dumps(self.to_dict(), separators=(",", ":"))


# Rating patterns ordered by specificity (longer/more specific first)
# This ensures NC-17 matches before N, PG-13 before PG, etc.
RATING_PATTERNS = [
    (r"\bNC[-\s]?17\b", "NC-17"),
    (r"\bPG[-\s]?13\b", "PG-13"),
    (r"\bPG\b", "PG"),
    (r"\bG[- ]?rated\b", "G"),
    (r"\brated[- ]?G\b", "G"),
    (r"\brating\s*[=:]\s*['\"]?G['\"]?\b", "G"),
    (r"\bG\b(?!\s*films?\s+rated)", "G"),  # Standalone G, not "G films rated"
    (r"\bR\b", "R"),
]

# Spelled-out number mappings
SPELLED_NUMBERS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "eleven": 11,
    "twelve": 12,
    "thirteen": 13,
    "fourteen": 14,
    "fifteen": 15,
    "sixteen": 16,
    "seventeen": 17,
    "eighteen": 18,
    "nineteen": 19,
    "twenty": 20,
}

# Build regex pattern for spelled numbers
SPELLED_NUM_PATTERN = "|".join(SPELLED_NUMBERS.keys())


def _parse_spelled_number(m) -> int:
    """Parse a spelled-out number from regex match."""
    word = m.group(1).lower()
    return SPELLED_NUMBERS.get(word, 0)


# Limit patterns
LIMIT_PATTERNS = [
    (r"\btop\s+(\d+)\b", lambda m: int(m.group(1))),
    (r"\b(\d+)\s+(?:best|top|highest|most)\b", lambda m: int(m.group(1))),
    (r"\blimit\s+(\d+)\b", lambda m: int(m.group(1))),
    (r"\bfirst\s+(\d+)\b", lambda m: int(m.group(1))),
    # Spelled-out numbers: "top ten", "top five", etc.
    (rf"\btop\s+({SPELLED_NUM_PATTERN})\b", _parse_spelled_number),
    (rf"\b({SPELLED_NUM_PATTERN})\s+(?:best|top|highest|most)\b", _parse_spelled_number),
]

# Ties patterns
TIES_PATTERNS = [
    r"\bincluding\s+ties\b",
    r"\bwith\s+ties\b",
    r"\binclude\s+ties\b",
]

# Entity patterns (actor, film, customer, etc.)
ENTITY_PATTERNS = [
    (r"\bactors?\b", "actor"),
    (r"\bfilms?\b", "film"),
    (r"\bmovies?\b", "film"),
    (r"\bcustomers?\b", "customer"),
    (r"\bstores?\b", "store"),
    (r"\bpayments?\b", "payment"),
    (r"\brentals?\b", "rental"),
]

# Metric patterns
METRIC_PATTERNS = [
    (r"\bcount\s+(?:of\s+)?distinct\b", "count_distinct"),
    (r"\bdistinct\s+count\b", "count_distinct"),
    (r"\bnumber\s+of\s+(?:unique|distinct)\b", "count_distinct"),
    (r"\bhow\s+many\s+(?:unique|distinct|different)\b", "count_distinct"),
    (r"\btotal\s+(?:number|count)\b", "count"),
    (r"\bcount\b", "count"),
    (r"\bsum\b", "sum"),
    (r"\baverage\b", "avg"),
    (r"\bavg\b", "avg"),
]


def extract_constraints(query: str) -> QueryConstraints:
    """
    Extract hard constraints from a natural language query.

    Uses deterministic regex patterns with priority ordering to identify:
    - Rating (NC-17, PG-13, PG, G, R)
    - Limit (top N, first N)
    - Include ties
    - Entity (actor, film, customer)
    - Metric (count_distinct, sum, avg)

    Args:
        query: Natural language query string

    Returns:
        QueryConstraints with extracted values and confidence score
    """
    constraints = QueryConstraints()
    confidence_factors = []

    # Extract rating (priority order: longer patterns first)
    for pattern, rating in RATING_PATTERNS:
        if re.search(pattern, query, re.IGNORECASE):
            constraints.rating = rating
            constraints._matched_patterns.append(f"rating:{pattern}")
            confidence_factors.append(1.0)
            break

    # If no rating found, reduce confidence
    if not constraints.rating:
        confidence_factors.append(0.3)

    # Extract limit
    for pattern, extractor in LIMIT_PATTERNS:
        match = re.search(pattern, query, re.IGNORECASE)
        if match:
            constraints.limit = extractor(match)
            constraints._matched_patterns.append(f"limit:{pattern}")
            break

    # Check for ties
    for pattern in TIES_PATTERNS:
        if re.search(pattern, query, re.IGNORECASE):
            constraints.include_ties = True
            constraints._matched_patterns.append(f"ties:{pattern}")
            break

    # Extract entity
    for pattern, entity in ENTITY_PATTERNS:
        if re.search(pattern, query, re.IGNORECASE):
            constraints.entity = entity
            constraints._matched_patterns.append(f"entity:{pattern}")
            break

    # Extract metric
    for pattern, metric in METRIC_PATTERNS:
        if re.search(pattern, query, re.IGNORECASE):
            constraints.metric = metric
            constraints._matched_patterns.append(f"metric:{pattern}")
            break

    # Calculate overall confidence
    if confidence_factors:
        constraints.confidence = sum(confidence_factors) / len(confidence_factors)
    else:
        constraints.confidence = 0.5

    return constraints


def normalize_rating(rating_str: str) -> Optional[str]:
    """
    Normalize rating string to canonical form.

    Handles variations like:
    - "pg" -> "PG"
    - "pg-13" -> "PG-13"
    - "pg 13" -> "PG-13"
    - "nc17" -> "NC-17"

    Args:
        rating_str: Raw rating string

    Returns:
        Canonical rating or None if unrecognized
    """
    if not rating_str:
        return None

    normalized = rating_str.upper().strip()

    # Handle hyphen/space variations
    normalized = re.sub(r"[-\s]+", "-", normalized)

    # Map to canonical forms
    rating_map = {
        "G": "G",
        "PG": "PG",
        "PG-13": "PG-13",
        "R": "R",
        "NC-17": "NC-17",
        "NC17": "NC-17",
    }

    return rating_map.get(normalized)
