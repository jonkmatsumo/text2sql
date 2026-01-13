import logging
from datetime import datetime, timezone
from typing import Any, List

from mcp_server.models import QueryPair
from mcp_server.services.recommendation.config import RECO_CONFIG
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
        fetch_limit = limit * RECO_CONFIG.candidate_multiplier

        approved = await RegistryService.lookup_semantic(
            question, tenant_id, role="example", status="verified", limit=fetch_limit
        )

        seeded = await RegistryService.lookup_semantic(
            question, tenant_id, role="example", status="seeded", limit=fetch_limit
        )

        # 2. Rank and Deduplicate
        all_candidates = approved + seeded
        # Centralized filtering hook
        filtered_candidates = RecommendationService._filter_invalid_candidates(
            all_candidates, RECO_CONFIG
        )
        recommended = RecommendationService._rank_and_deduplicate(
            filtered_candidates, limit, RECO_CONFIG
        )

        fallback_used = False
        # 3. Fallback Path (enabled by both arg AND config)
        effective_fallback_enabled = enable_fallback and RECO_CONFIG.fallback_enabled

        if len(recommended) < limit and effective_fallback_enabled:
            # For fallback, we look at successful interactions
            # Note: We currently don't have a specific status for 'success' in QueryPair
            # in a way that matches this exactly, but we can look for role='interaction'
            # and potentially a high similarity threshold.
            history = await RegistryService.lookup_semantic(
                question,
                tenant_id,
                role="interaction",
                threshold=RECO_CONFIG.fallback_threshold,
                limit=limit,
            )

            if history:
                # Filter fallback candidates too
                filtered_history = RecommendationService._filter_invalid_candidates(
                    history, RECO_CONFIG
                )

                if filtered_history:
                    fallback_used = True
                    # Deduplicate history against already picked
                    picked_fingerprints = {
                        ex.canonical_group_id for ex in recommended if ex.canonical_group_id
                    }
                    for h in filtered_history:
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
    def _filter_invalid_candidates(candidates: List[QueryPair], config: Any) -> List[QueryPair]:
        """Filter out candidates based on validity and staleness rules.

        Contract:
        - Exclude tombstoned examples (if configured).
        - Exclude incomplete examples (missing question, sql, or fingerprint).
        - Exclude stale examples (if staleness filtering enabled).
        - Applied uniformly to ALL candidate sources (verified, seeded, fallback).
        - Fail-safe: Returns valid subset, never raises.
        """
        if not candidates:
            return []

        filtered = []
        try:
            # Safe access to config attributes
            exclude_tombstoned = getattr(config, "exclude_tombstoned", True)
            stale_max_age_days = getattr(config, "stale_max_age_days", 0)

            for cp in candidates:
                # 1. Check Tombstone
                if exclude_tombstoned and cp.status == "tombstoned":
                    logger.debug(f"Filtering tombstoned candidate: {cp.fingerprint}")
                    continue

                # 2. Check Required Fields
                # question, sql_query, fingerprint must be non-empty
                if not cp.question or not cp.sql_query or not cp.fingerprint:
                    logger.debug(f"Filtering incomplete candidate: {cp.fingerprint}")
                    continue

                # 3. Check Staleness
                if stale_max_age_days > 0:
                    if not cp.updated_at:
                        logger.debug(
                            "Filtering candidate missing updated_at (staleness enabled): "
                            f"{cp.fingerprint}"
                        )
                        continue

                    # Ensure updated_at is timezone-aware or assume proper comparison
                    now = datetime.now(timezone.utc)

                    # Handle timezone awareness of cp.updated_at
                    ex_time = cp.updated_at
                    if ex_time.tzinfo is None:
                        # If naive, assume UTC (standard practice in this project)
                        ex_time = ex_time.replace(tzinfo=timezone.utc)

                    age = now - ex_time
                    if age.total_seconds() > (stale_max_age_days * 86400):
                        logger.debug(
                            f"Filtering stale candidate ({age.days} days old): " f"{cp.fingerprint}"
                        )
                        continue

                filtered.append(cp)
        except Exception as e:
            # Guardrail: filtering must never raise exceptions
            logger.error(f"Unexpected error during validity filtering: {e}", exc_info=True)
            # In case of error, we return the candidates that were already successfully filtered
            pass

        return filtered

    @staticmethod
    def _rank_and_deduplicate(
        candidates: List[QueryPair], limit: int, config: Any = None
    ) -> List[RecommendedExample]:
        """Rank candidates and enforce diversity (one per canonical group)."""
        ranked = RecommendationService._rank_candidates(candidates)
        deduped = RecommendationService._dedupe_by_fingerprint(ranked)
        diversified = RecommendationService._apply_diversity_policy(deduped, limit, config)
        return RecommendationService._select_top_n(diversified, limit)

    @staticmethod
    def _rank_candidates(candidates: List[QueryPair]) -> List[QueryPair]:
        """Rank candidates by status priority and existing order (similarity)."""
        # Build priority map from config
        # Lower index = higher priority
        priority_map = {status: i for i, status in enumerate(RECO_CONFIG.status_priority)}
        # Unknown statuses get pushed to the end
        default_priority = len(RECO_CONFIG.status_priority)

        def sort_key(cp: QueryPair):
            # Status priority from config
            status_pri = priority_map.get(cp.status, default_priority)
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
        candidates: List[QueryPair], limit: int, config: Any = None
    ) -> List[QueryPair]:
        """Apply diversity selection policy.

        Expected inputs:
        - candidates: List of QueryPair objects, ranked by primary criteria.
        - limit: Maximum number of candidates to return.
        - config: Configuration object (RecommendationConfig) or dict.

        Preserved invariants:
        - Output subset of inputs.
        - Order matches selection order (stable relative to inputs where posssible).
        - Fingerprint uniqueness (already enforced, but preserved here).

        Future extension points:
        - Add 'diversity_weights' to config for score adjustment.
        - Support additional dimensions beyond 'source'.
        """
        if not config or not getattr(config, "diversity_enabled", False):
            return candidates

        max_per_source = getattr(config, "diversity_max_per_source", -1)
        if not isinstance(max_per_source, int) or max_per_source < -1:
            logger.warning(
                f"Invalid diversity_max_per_source: {max_per_source}. Disabling diversity."
            )
            return candidates

        min_verified = getattr(config, "diversity_min_verified", 0)
        if not isinstance(min_verified, int) or min_verified < 0:
            logger.warning(f"Invalid diversity_min_verified: {min_verified}. Disabling diversity.")
            return candidates

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
