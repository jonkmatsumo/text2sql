import logging
import re
from datetime import datetime, timezone
from typing import Any, List, Optional

from common.sanitization import sanitize_text
from mcp_server.models import QueryPair
from mcp_server.services.recommendation.config import RECO_CONFIG
from mcp_server.services.recommendation.explanation import (
    DiversityExplanation,
    FilteringExplanation,
    RecommendationExplanation,
)
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

        Guarantees:
        - When diversity is disabled (default), returns top candidates by status priority
          and similarity.
        - When diversity is enabled, guarantees a best-effort distribution across
          sources (approved, seeded, fallback) based on configured floors and caps.
        - Fingerprint uniqueness (one example per canonical ID) is ALWAYS enforced.
        - Fallback examples participate in diversity policies when triggered.
        - Selection is deterministic based on input ranking.

        Explicitly NOT Guaranteed:
        - No guarantee of diversity in SQL query structure or logic.
        - No guarantee that 'limit' is reached if available candidates are exhausted
          by source caps and floors.
        - Similarity ordering within a single source bucket may be skewed by diversity floors.

        Algorithm:
        1. Fetch and resolve Pinned Examples.
        2. Fetch 'verified' and 'seeded' examples.
        3. Filter invalid and Deduplicate against pins.
        4. Rank, Dedupe, and Apply Diversity to remaining candidates.
        5. Fallback if insufficient.
        """
        # 0. Initialize Explanation
        explanation = RecommendationExplanation()

        # 0. Pinned Examples
        pin_rules = await RecommendationService._match_pin_rules(question, tenant_id)
        pinned_examples = await RecommendationService._resolve_pins(pin_rules, tenant_id)
        selected_fingerprints = {
            ex.canonical_group_id for ex in pinned_examples if ex.canonical_group_id
        }

        explanation.pins.matched_rules = [str(r.id) for r in pin_rules]
        explanation.pins.selected_count = len(pinned_examples)

        # 1. Fetch Candidates (Normal)
        fetch_limit = limit * RECO_CONFIG.candidate_multiplier
        approved = await RegistryService.lookup_semantic(
            question, tenant_id, role="example", status="verified", limit=fetch_limit
        )

        seeded = await RegistryService.lookup_semantic(
            question, tenant_id, role="example", status="seeded", limit=fetch_limit
        )

        # 2. Rank and Deduplicate (with Pins Pre-filled)
        all_candidates = approved + seeded
        explanation.selection_summary.total_candidates = len(all_candidates) + len(pinned_examples)
        explanation.selection_summary.counts_by_source["approved"] = len(approved)
        explanation.selection_summary.counts_by_source["seeded"] = len(seeded)

        # Centralized filtering hook
        filtered_candidates = RecommendationService._filter_invalid_candidates(
            all_candidates, RECO_CONFIG, explanation.filtering
        )

        # Exclude already pinned fingerprints
        filtered_candidates = [
            cp for cp in filtered_candidates if cp.fingerprint not in selected_fingerprints
        ]

        remaining_limit = max(0, limit - len(pinned_examples))
        dynamic_recos = RecommendationService._rank_and_deduplicate(
            filtered_candidates, remaining_limit, RECO_CONFIG, explanation.diversity
        )

        recommended = pinned_examples + dynamic_recos
        total_valid_candidates = len(filtered_candidates) + len(pinned_examples)

        fallback_used = False
        # 3. Fallback Path (enabled by both arg AND config)
        effective_fallback_enabled = enable_fallback and RECO_CONFIG.fallback_enabled

        if len(recommended) < limit and effective_fallback_enabled:
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
                    history, RECO_CONFIG, explanation.filtering
                )
                if filtered_history:
                    total_valid_candidates += len(filtered_history)
                    explanation.selection_summary.total_candidates += len(history)
                    explanation.selection_summary.counts_by_source["interactions"] = len(history)

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

                fallback_used = True
                explanation.fallback.used = True
                explanation.fallback.reason = "insufficient_verified_candidates"

        explanation.fallback.enabled = effective_fallback_enabled
        explanation.fallback.candidate_multiplier = RECO_CONFIG.candidate_multiplier
        explanation.fallback.shortage_count = max(0, limit - len(recommended))

        # Build Telemetry Metadata
        explanation.selection_summary.returned_count = len(recommended)
        for ex in recommended:
            explanation.selection_summary.counts_by_status[ex.source] = (
                explanation.selection_summary.counts_by_status.get(ex.source, 0) + 1
            )
        metadata = {
            "count_total": len(recommended),
            "count_approved": sum(1 for ex in recommended if ex.source == "approved"),
            "count_seeded": sum(1 for ex in recommended if ex.source == "seeded"),
            "count_fallback": sum(1 for ex in recommended if ex.source == "fallback"),
            "fingerprints": [ex.canonical_group_id for ex in recommended],
            "sources": [ex.source for ex in recommended],
            "statuses": [ex.metadata.get("status") or ex.source for ex in recommended],
            "positions": list(range(len(recommended))),
            "truncated": len(recommended) >= limit and total_valid_candidates > len(recommended),
            "pins_matched_rules": [str(r.id) for r in pin_rules],
            "pins_selected_count": len(pinned_examples),
        }

        return RecommendationResult(
            examples=recommended,
            fallback_used=fallback_used,
            metadata=metadata,
            explanation=explanation,
        )

    @staticmethod
    def _filter_invalid_candidates(
        candidates: List[QueryPair], config: Any, explanation: Optional[FilteringExplanation] = None
    ) -> List[QueryPair]:
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
                    if explanation:
                        explanation.tombstoned_removed += 1
                    continue

                # 2. Check Required Fields
                # question, sql_query, fingerprint must be non-empty
                if not cp.question or not cp.sql_query or not cp.fingerprint:
                    logger.debug(f"Filtering incomplete candidate: {cp.fingerprint}")
                    if explanation:
                        explanation.missing_fields_removed += 1
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
                        if explanation:
                            explanation.stale_removed += 1
                        continue

                # 4. Check Safety Rules (Issue #119)
                if getattr(config, "safety_enabled", False):
                    # a. Length check (config-based)
                    max_len = getattr(config, "safety_max_pattern_length", 100)
                    if len(cp.question) > max_len:
                        logger.debug(f"Safety filtering: Question too long ({len(cp.question)}).")
                        if explanation:
                            explanation.safety_removed += 1
                        continue

                    # b. Regex Blocklist
                    blocklist_regex = getattr(config, "safety_blocklist_regex", None)
                    if blocklist_regex:
                        try:
                            if re.search(blocklist_regex, cp.question, re.IGNORECASE):
                                logger.debug(
                                    f"Safety filtering: Blocklist match for {cp.fingerprint}."
                                )
                                if explanation:
                                    explanation.safety_removed += 1
                                continue
                        except re.error as e:
                            logger.error(f"Invalid safety blocklist regex: {e}")

                    # c. Sanitizer (if required)
                    if getattr(config, "safety_require_sanitizable", True):
                        res = sanitize_text(cp.question)
                        if not res.is_valid:
                            logger.debug(
                                f"Safety filtering: Invalid following sanitization: {res.errors}"
                            )
                            if explanation:
                                explanation.safety_removed += 1
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
        candidates: List[QueryPair],
        limit: int,
        config: Any = None,
        explanation: Optional[DiversityExplanation] = None,
    ) -> List[RecommendedExample]:
        """Rank candidates and enforce diversity (one per canonical group)."""
        ranked = RecommendationService._rank_candidates(candidates)
        deduped = RecommendationService._dedupe_by_fingerprint(ranked)
        diversified = RecommendationService._apply_diversity_policy(
            deduped, limit, config, explanation
        )
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
        candidates: List[QueryPair],
        limit: int,
        config: Any = None,
        explanation: Optional[DiversityExplanation] = None,
    ) -> List[QueryPair]:
        """Apply diversity selection policy.

        This policy is applied AFTER ranking and fingerprint deduplication. It defines
        the final "mix" of examples returned to the user.

        Invariants Preserved:
        - Result is always a subset of input candidates.
        - Order matches selection order (Pass A then Pass B).
        - Fingerprint uniqueness (already enforced but preserved).
        - Never returns more than 'limit'.

        Selection Passes:
        1. Pass A (Verified Floor): Pulls up to 'diversity_min_verified' examples
           from the 'approved' source regardless of their original rank position.
        2. Pass B (Fill Remaining): Fills the remaining capacity to reach 'limit' while
           enforcing 'diversity_max_per_source' caps across all sources (approved,
           seeded, fallback).

        Note: If diversity is disabled or config is invalid, this is a passthrough.

        Future-Proofing & Extensions:
        - SQL-structure diversity is currently OUT OF SCOPE. This would require
          SQL parsing/fingerprinting beyond canonical GIDs.
        - Tests (Issue #111) should assert source distribution but NOT strict
          similarity ordering, as diversity floors (Pass A) explicitly break
          pure similarity preference to ensure mix.
        - New heuristics (e.g. schema overlap, keyword diversity) should plug in
          as additional passes between Pass A and Pass B.
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

        if explanation:
            explanation.enabled = True
            explanation.min_verified = min_verified
            explanation.max_per_source = max_per_source
            explanation.applied = True

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
                        if explanation:
                            explanation.effects.verified_floor_applied = True

        # Pass B: Fill Remaining
        for cp in candidates:
            if len(selected) >= limit:
                break

            if cp.fingerprint in selected_fingerprints:
                continue

            source = get_source(cp)

            # Check cap (if applicable)
            if max_per_source != -1 and source_counts.get(source, 0) >= max_per_source:
                if explanation:
                    explanation.effects.source_caps_applied[source] = (
                        explanation.effects.source_caps_applied.get(source, 0) + 1
                    )
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
                    metadata={"status": cp.status},
                )
            )
        return recommended

    @staticmethod
    async def _match_pin_rules(question: str, tenant_id: int):
        from dal.postgres.pinned_recommendations import PostgresPinnedRecommendationStore

        store = PostgresPinnedRecommendationStore()
        rules = await store.list_rules(tenant_id, only_enabled=True)

        matches = []
        q_norm = question.lower().strip()
        for r in rules:
            if r.match_type == "exact" and r.match_value.lower() == q_norm:
                matches.append(r)
            elif r.match_type == "contains" and r.match_value.lower() in q_norm:
                matches.append(r)

        # Sort by priority desc
        return sorted(matches, key=lambda x: x.priority, reverse=True)

    @staticmethod
    async def _resolve_pins(rules, tenant_id: int) -> List[RecommendedExample]:
        if not rules:
            return []

        sig_to_rule_meta = {}
        all_sigs = []

        for r in rules:
            for sig in r.registry_example_ids:
                if sig not in sig_to_rule_meta:
                    sig_to_rule_meta[sig] = {"rule_id": str(r.id), "priority": r.priority}
                    all_sigs.append(sig)

        if not all_sigs:
            return []

        pairs = await RegistryService.fetch_by_signatures(all_sigs, tenant_id)
        pair_map = {p.signature_key: p for p in pairs}

        pinned_examples = []

        for sig in all_sigs:
            if sig in pair_map:
                pair = pair_map[sig]
                # Safety: Skip tombstones
                if pair.status == "tombstoned":
                    continue

                meta = sig_to_rule_meta[sig]

                reco_meta = pair.metadata.copy() if pair.metadata else {}
                reco_meta.update(
                    {
                        "pinned": True,
                        "pin_rule_id": meta["rule_id"],
                        "pin_priority": meta["priority"],
                    }
                )

                pinned_examples.append(
                    RecommendedExample(
                        question=pair.question,
                        sql=pair.sql_query,
                        score=2.0,
                        source="pinned",
                        canonical_group_id=pair.fingerprint,
                        metadata=reco_meta,
                    )
                )

        return pinned_examples
