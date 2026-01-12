import logging
from typing import List

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
    def _rank_and_deduplicate(candidates: List[QueryPair], limit: int) -> List[RecommendedExample]:
        """Rank candidates and enforce diversity (one per canonical group)."""
        # Sort by status priority (verified > seeded) and similarity (if available)
        # QueryPair doesn't explicitly store similarity from lookup_semantic in its model,
        # but the DAL returns them in that order.

        def sort_key(cp: QueryPair):
            # Status priority: verified (0) > seeded (1) > others (2)
            status_pri = 0 if cp.status == "verified" else (1 if cp.status == "seeded" else 2)
            return status_pri

        # Candidates from lookup_semantic are already sorted by similarity.
        # We'll use a stable sort to keep similarity ordering for same status.
        sorted_candidates = sorted(candidates, key=sort_key)

        recommended: List[RecommendedExample] = []
        seen_fingerprints = set()

        for cp in sorted_candidates:
            if len(recommended) >= limit:
                break

            if cp.fingerprint not in seen_fingerprints:
                recommended.append(
                    RecommendedExample(
                        question=cp.question,
                        sql=cp.sql_query,
                        score=1.0,  # Placeholder
                        source="approved" if cp.status == "verified" else "seeded",
                        canonical_group_id=cp.fingerprint,
                    )
                )
                seen_fingerprints.add(cp.fingerprint)

        return recommended
