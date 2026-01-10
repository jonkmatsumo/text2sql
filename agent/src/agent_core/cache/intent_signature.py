"""Intent signature for exact-match cache keying.

This module defines a canonical representation of query intent that
enables exact-match cache lookups. Unlike semantic similarity, signature
matching guarantees that PG != G != R.
"""

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class IntentSignature:
    """Canonical representation of query intent for cache keying.

    Attributes:
        intent: High-level intent name (e.g., "top_actors_by_film_count")
        entity: Primary entity (e.g., "actor")
        item: Secondary entity (e.g., "film")
        metric: Aggregation type (e.g., "count_distinct")
        filters: Hard filter constraints (e.g., {"rating": "G"})
        ranking: Ranking parameters (e.g., {"limit": 10, "include_ties": True})
    """

    intent: Optional[str] = None
    entity: Optional[str] = None
    item: Optional[str] = None
    metric: Optional[str] = None
    filters: Dict[str, str] = field(default_factory=dict)
    ranking: Dict[str, Any] = field(default_factory=dict)

    def to_canonical_json(self) -> str:
        """Convert to canonical JSON with stable key ordering.

        Returns:
            JSON string with sorted keys, lowercase values, no extra whitespace.
        """
        # Build canonical dict with only non-None values
        canonical = {}
        if self.intent:
            canonical["intent"] = self.intent.lower()
        if self.entity:
            canonical["entity"] = self.entity.lower()
        if self.item:
            canonical["item"] = self.item.lower()
        if self.metric:
            canonical["metric"] = self.metric.lower()
        if self.filters:
            # Sort filter keys and normalize values
            canonical["filters"] = {
                k.lower(): v.upper() if k == "rating" else str(v).lower()
                for k, v in sorted(self.filters.items())
            }
        if self.ranking:
            canonical["ranking"] = {k: v for k, v in sorted(self.ranking.items())}

        return json.dumps(canonical, sort_keys=True, separators=(",", ":"))

    def compute_key(self) -> str:
        """Compute SHA256 hash of canonical JSON as cache key.

        Returns:
            64-character hex string.
        """
        canonical = self.to_canonical_json()
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def to_dict(self) -> dict:
        """Convert to dictionary for storage/logging."""
        return {
            "intent": self.intent,
            "entity": self.entity,
            "item": self.item,
            "metric": self.metric,
            "filters": self.filters,
            "ranking": self.ranking,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "IntentSignature":
        """Create IntentSignature from dictionary."""
        return cls(
            intent=data.get("intent"),
            entity=data.get("entity"),
            item=data.get("item"),
            metric=data.get("metric"),
            filters=data.get("filters", {}),
            ranking=data.get("ranking", {}),
        )


def build_signature_from_constraints(
    query: str,
    rating: Optional[str] = None,
    limit: Optional[int] = None,
    include_ties: bool = False,
    entity: Optional[str] = None,
    metric: Optional[str] = None,
) -> IntentSignature:
    """Build an IntentSignature from extracted constraints.

    Args:
        query: Original query (used for intent inference).
        rating: Extracted rating constraint.
        limit: Extracted limit constraint.
        include_ties: Whether ties are included.
        entity: Primary entity (actor, film, etc.).
        metric: Aggregation metric.

    Returns:
        IntentSignature with populated fields.
    """
    # Infer intent from query patterns
    intent = None
    query_lower = query.lower()
    if "top" in query_lower and entity == "actor":
        if "film" in query_lower or "movie" in query_lower:
            intent = "top_actors_by_film_count"
        else:
            intent = "top_actors"
    elif "top" in query_lower and entity == "film":
        intent = "top_films"

    # Build filters
    filters = {}
    if rating:
        filters["rating"] = rating

    # Build ranking
    ranking = {}
    if limit:
        ranking["limit"] = limit
    if include_ties:
        ranking["include_ties"] = include_ties

    return IntentSignature(
        intent=intent,
        entity=entity,
        item="film" if "film" in query_lower or "movie" in query_lower else None,
        metric=metric,
        filters=filters,
        ranking=ranking,
    )
