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
    """Canonical representation of query intent for cache keying."""

    intent: Optional[str] = None
    entity: Optional[str] = None
    item: Optional[str] = None
    metric: Optional[str] = None
    filters: Dict[str, str] = field(default_factory=dict)
    ranking: Dict[str, Any] = field(default_factory=dict)

    def to_canonical_json(self) -> str:
        """Convert to canonical JSON with stable key ordering."""
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
            canonical["filters"] = {
                k.lower(): v.upper() if k == "rating" else str(v).lower()
                for k, v in sorted(self.filters.items())
            }
        if self.ranking:
            # ranking includes limit, include_ties, sort_direction
            canonical["ranking"] = {
                k.lower(): v.upper() if k == "sort_direction" else v
                for k, v in sorted(self.ranking.items())
            }

        return json.dumps(canonical, sort_keys=True, separators=(",", ":"))

    def compute_key(self) -> str:
        """Compute SHA256 hash of canonical JSON as cache key."""
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
    sort_direction: Optional[str] = None,
    include_ties: bool = False,
    entity: Optional[str] = None,
    metric: Optional[str] = None,
) -> IntentSignature:
    """Build an IntentSignature from extracted constraints."""
    intent = None
    query_lower = query.lower()
    if "top" in query_lower and entity == "actor":
        if "film" in query_lower or "movie" in query_lower:
            intent = "top_actors_by_film_count"
        else:
            intent = "top_actors"
    elif "top" in query_lower and entity == "film":
        intent = "top_films"

    filters = {}
    if rating:
        filters["rating"] = rating

    ranking = {}
    if limit:
        ranking["limit"] = limit
    if sort_direction:
        ranking["sort_direction"] = sort_direction
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
