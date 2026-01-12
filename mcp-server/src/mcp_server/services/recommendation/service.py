import logging
from typing import Any, Dict, List

from mcp_server.models import QueryPair
from mcp_server.services.recommendation.interface import RecommendationResult, RecommendedExample
from mcp_server.services.registry import RegistryService

logger = logging.getLogger(__name__)


class RecommendationService:
    """Service to recommend few-shot examples using ranking and diversity rules."""

    @staticmethod
    async def recommend_examples(
        question: str,
        tenant_id: int,
        limit: int = 3,
        enable_fallback: bool = True,
    ) -> RecommendationResult:
        """Recommend few-shot examples for a given question.

        Algorithm:
        1. Fetch 'verified' examples (approved).
        2. Fetch 'seeded' examples.
        3. Rank and Deduplicate by canonical group (fingerprint).
        4. If insufficient and enabled, fetch from interaction history (fallback).
        """
        # 1. Fetch Candidates
        approved = await RegistryService.lookup_semantic(
            question, tenant_id, role="example", status="verified", limit=limit * 2
        )

        seeded = await RegistryService.lookup_semantic(
            question, tenant_id, role="example", status="seeded", limit=limit * 2
        )

        # 2. Rank and Deduplicate
        all_candidates = approved + seeded
        recommended = RecommendationService._rank_and_deduplicate(all_candidates, limit)

        fallback_used = False
        # 3. Fallback Path
        if len(recommended) < limit and enable_fallback:
            # For fallback, we look at successful interactions
            # Note: We currently don't have a specific status for 'success' in QueryPair
            # in a way that matches this exactly, but we can look for role='interaction'
            # and potentially a high similarity threshold.
            history = await RegistryService.lookup_semantic(
                question, tenant_id, role="interaction", threshold=0.85, limit=limit
            )

            if history:
                fallback_used = True
                # Deduplicate history against already picked
                picked_fingerprints = {
                    ex.canonical_group_id for ex in recommended if ex.canonical_group_id
                }
                for h in history:
                    if len(recommended) >= limit:
                        break
                    if h.fingerprint not in picked_fingerprints:
                        recommended.append(
                            RecommendedExample(
                                question=h.question,
                                sql=h.sql_query,
                                score=1.0,  # Placeholder score
                                source="fallback",
                                canonical_group_id=h.fingerprint,
                            )
                        )
                        picked_fingerprints.add(h.fingerprint)

        return RecommendationResult(examples=recommended, fallback_used=fallback_used)

    @staticmethod
    def _rank_and_deduplicate(
        candidates: List[QueryPair], limit: int, config: Dict[str, Any] = None
    ) -> List[RecommendedExample]:
        """Rank candidates and enforce diversity (one per canonical group)."""
        ranked = RecommendationService._rank_candidates(candidates)
        deduped = RecommendationService._dedupe_by_fingerprint(ranked)
        diversified = RecommendationService._apply_diversity_policy(deduped, limit, config)
        return RecommendationService._select_top_n(diversified, limit)

    @staticmethod
    def _rank_candidates(candidates: List[QueryPair]) -> List[QueryPair]:
        """Rank candidates by status priority and existing order (similarity)."""

        def sort_key(cp: QueryPair):
            # Status priority: verified (0) > seeded (1) > others (2)
            status_pri = 0 if cp.status == "verified" else (1 if cp.status == "seeded" else 2)
            return status_pri

        # Candidates from lookup_semantic are already sorted by similarity.
        # We'll use a stable sort to keep similarity ordering for same status.
        return sorted(candidates, key=sort_key)

    @staticmethod
    def _dedupe_by_fingerprint(candidates: List[QueryPair]) -> List[QueryPair]:
        """Deduplicate candidates by fingerprint, keeping the first occurrence."""
        seen_fingerprints = set()
        deduped = []
        for cp in candidates:
            if cp.fingerprint not in seen_fingerprints:
                deduped.append(cp)
                seen_fingerprints.add(cp.fingerprint)
        return deduped

    @staticmethod
    def _apply_diversity_policy(
        candidates: List[QueryPair], limit: int, config: Dict[str, Any] = None
    ) -> List[QueryPair]:
        """Apply diversity selection policy."""
        if not config or not config.get("diversity_enabled", False):
            return candidates

        max_per_source = config.get("diversity_max_per_source", -1)
        min_verified = config.get("diversity_min_verified", 0)

        selected: List[QueryPair] = []
        source_counts = {"approved": 0, "seeded": 0, "fallback": 0}
        selected_fingerprints = set()

        def get_source(cp: QueryPair) -> str:
            if cp.status == "verified":
                return "approved"
            if cp.status == "seeded":
                return "seeded"
            return "fallback"

        # Pass A: Verified Floor
        for cp in candidates:
            source = get_source(cp)
            if source == "approved":
                if source_counts["approved"] < min_verified:
                    # Check cap (if applicable, though unlikely to hit cap while meeting min floor
                    # unless config is conflicting)
                    if max_per_source == -1 or source_counts["approved"] < max_per_source:
                        selected.append(cp)
                        source_counts["approved"] += 1
                        selected_fingerprints.add(cp.fingerprint)

        # Pass B: Fill Remaining
        for cp in candidates:
            if len(selected) >= limit:
                break

            if cp.fingerprint in selected_fingerprints:
                continue

            source = get_source(cp)

            # Check cap (if applicable)
            if max_per_source != -1 and source_counts.get(source, 0) >= max_per_source:
                continue

            selected.append(cp)
            source_counts[source] = source_counts.get(source, 0) + 1
            selected_fingerprints.add(cp.fingerprint)

        return selected

    @staticmethod
    def _select_top_n(candidates: List[QueryPair], limit: int) -> List[RecommendedExample]:
        """Select top N candidates and convert to RecommendedExample."""
        recommended = []
        for cp in candidates:
            if len(recommended) >= limit:
                break

            recommended.append(
                RecommendedExample(
                    question=cp.question,
                    sql=cp.sql_query,
                    score=1.0,  # Placeholder
                    source="approved" if cp.status == "verified" else "seeded",
                    canonical_group_id=cp.fingerprint,
                )
            )
        return recommended
