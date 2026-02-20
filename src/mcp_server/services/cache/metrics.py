"""Cache metrics for monitoring and observability.

Provides counters and histograms for cache operations including:
- Guardrail failures (when cached SQL doesn't match constraints)
- Tombstones created (invalidated cache entries)
- Regeneration rate (cache miss ratio)
- Extraction failures (when constraint extraction fails)
- Semantic ambiguity (when top-1 and top-2 similarity are too close)
"""

import logging
from dataclasses import dataclass, field
from typing import Optional

from common.observability.metrics import mcp_metrics

logger = logging.getLogger(__name__)


@dataclass
class CacheMetrics:
    """In-memory cache metrics for monitoring."""

    # Counters
    guardrail_failures: int = 0
    tombstones_created: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    extraction_failures: int = 0
    semantic_ambiguity_count: int = 0

    # Recent values for debugging
    last_mismatch_details: Optional[dict] = None
    last_similarity_margin: Optional[float] = None

    # Aggregations
    _similarity_scores: list = field(default_factory=list)
    _regeneration_times_ms: list = field(default_factory=list)

    def record_guardrail_failure(self, details: dict) -> None:
        """Record a guardrail failure (constraint mismatch)."""
        self.guardrail_failures += 1
        self.last_mismatch_details = details
        logger.warning(f"Guardrail failure: {details}")
        mcp_metrics.add_counter(
            "mcp.cache.guardrail_failures_total",
            description="Count of semantic cache guardrail failures",
        )

    def record_tombstone(self, cache_id: str, reason: str) -> None:
        """Record a tombstoned cache entry."""
        self.tombstones_created += 1
        logger.info(f"Tombstoned cache entry {cache_id}: {reason}")
        mcp_metrics.add_counter(
            "mcp.cache.tombstones_total",
            description="Count of semantic cache tombstones created",
        )

    def record_cache_hit(self, similarity: float) -> None:
        """Record a successful cache hit."""
        self.cache_hits += 1
        self._similarity_scores.append(similarity)
        mcp_metrics.add_counter(
            "mcp.cache.hits_total",
            description="Count of semantic cache hits",
        )
        mcp_metrics.record_histogram(
            "mcp.cache.hit_similarity",
            float(similarity),
            description="Similarity score distribution for semantic cache hits",
        )

    def record_cache_miss(self) -> None:
        """Record a cache miss."""
        self.cache_misses += 1
        mcp_metrics.add_counter(
            "mcp.cache.misses_total",
            description="Count of semantic cache misses",
        )

    def record_extraction_failure(self, query: str, error: str) -> None:
        """Record a constraint extraction failure."""
        self.extraction_failures += 1
        logger.warning(f"Extraction failure for '{query}': {error}")
        mcp_metrics.add_counter(
            "mcp.cache.extraction_failures_total",
            description="Count of semantic cache extraction failures",
        )

    def record_semantic_ambiguity(self, top1: float, top2: float) -> None:
        """Record when top-1 and top-2 similarity are too close."""
        margin = top1 - top2
        self.semantic_ambiguity_count += 1
        self.last_similarity_margin = margin
        logger.info(f"Semantic ambiguity detected: margin={margin:.4f}")
        mcp_metrics.add_counter(
            "mcp.cache.semantic_ambiguity_total",
            description="Count of semantic ambiguity events in cache retrieval",
        )
        mcp_metrics.record_histogram(
            "mcp.cache.semantic_margin",
            float(margin),
            description="Similarity margin between top-1 and top-2 cache candidates",
        )

    def get_regeneration_rate(self) -> float:
        """Calculate cache miss ratio."""
        total = self.cache_hits + self.cache_misses
        if total == 0:
            return 0.0
        return self.cache_misses / total

    def get_avg_similarity(self) -> float:
        """Calculate average similarity score for hits."""
        if not self._similarity_scores:
            return 0.0
        return sum(self._similarity_scores) / len(self._similarity_scores)

    def to_dict(self) -> dict:
        """Convert metrics to dictionary for reporting."""
        return {
            "guardrail_failures": self.guardrail_failures,
            "tombstones_created": self.tombstones_created,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "extraction_failures": self.extraction_failures,
            "semantic_ambiguity_count": self.semantic_ambiguity_count,
            "regeneration_rate": self.get_regeneration_rate(),
            "avg_similarity": self.get_avg_similarity(),
        }

    def reset(self) -> None:
        """Reset all counters (for testing)."""
        self.guardrail_failures = 0
        self.tombstones_created = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.extraction_failures = 0
        self.semantic_ambiguity_count = 0
        self._similarity_scores.clear()
        self._regeneration_times_ms.clear()


# Global metrics instance
_metrics = CacheMetrics()


def get_cache_metrics() -> CacheMetrics:
    """Get the global cache metrics instance."""
    return _metrics


def reset_cache_metrics() -> None:
    """Reset global cache metrics (for testing)."""
    _metrics.reset()
