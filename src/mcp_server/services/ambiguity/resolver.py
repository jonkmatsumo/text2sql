"""Decision engine for ambiguity resolution.

Uses CandidateBinder to evaluate potential bindings and applies a margin-based
policy to decide whether to silently resolve or ask for clarification.
"""

import logging
from typing import Any, Dict, List

from common.config.env import get_env_float
from mcp_server.services.ambiguity.binder import CandidateBinder, MentionExtractor

logger = logging.getLogger(__name__)

# Configurable thresholds
DELTA_THRESHOLD = get_env_float("AMBIGUITY_DELTA", 0.10)
MIN_BINDING_SCORE = get_env_float("AMBIGUITY_MIN_SCORE", 0.70)
HIGH_CONFIDENCE_THRESHOLD = get_env_float("AMBIGUITY_HIGH_CONFIDENCE", 0.92)


class AmbiguityResolver:
    """Orchestrates mention extraction and candidate selection."""

    def __init__(self):
        """Initialize resolver components."""
        self.extractor = MentionExtractor()
        self.binder = CandidateBinder()

    def resolve(self, query: str, schema_context: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Resolve ambiguities in the user query against the schema context.

        Args:
            query: The user's natural language question.
            schema_context: List of retrieved schema nodes (tables + cols).

        Returns:
            Dict containing:
                - status: 'CLEAR', 'AMBIGUOUS', or 'MISSING'
                - resolved_bindings: Map of mention -> canonical_id (if CLEAR)
                - ambiguities: List of (mention, candidates) (if AMBIGUOUS)
        """
        mentions = self.extractor.extract(query)
        if not mentions:
            return {
                "status": "CLEAR",
                "resolved_bindings": {},
                "ambiguities": [],
                "grounding_metadata": {
                    "ent_id_present": False,
                    "ontology_match_used": False,
                    "schema_candidates_count": 0,
                },
            }

        resolved_bindings = {}
        ambiguities = []
        missing = []

        # === Grounding Metadata (Phase C telemetry) ===
        grounding_metadata = {
            "ent_id_present": False,
            "ontology_match_used": False,
            "schema_candidates_count": 0,
        }

        for m in mentions:
            # Track ent_id presence
            if m.metadata.get("ent_id"):
                grounding_metadata["ent_id_present"] = True

            candidates = self.binder.get_candidates(m, schema_context)
            grounding_metadata["schema_candidates_count"] += len(candidates)

            if not candidates:
                missing.append(m.text)
                continue

            best = candidates[0]

            # Track ontology_match usage
            if best.scores.get("ontology_match") == 1.0:
                grounding_metadata["ontology_match_used"] = True

            # 1. Check minimum fitness
            if best.final_score < MIN_BINDING_SCORE:
                missing.append(m.text)
                continue

            # 2. Check margin for ambiguity

            if len(candidates) > 1:
                second = candidates[1]
                margin = best.final_score - second.final_score

                # CHECK 1: High Confidence Bypass
                # If the top candidate is extremely confident, we assume it's correct
                # even if there's a close runner-up (unless the runner-up is also identical score)
                if best.final_score >= HIGH_CONFIDENCE_THRESHOLD:
                    resolved_bindings[m.text] = best.id
                    continue

                # CHECK 2: Structural Cousin Bypass
                # If strict table vs PK of that table, don't block.
                # Downstream SQL generation handles this safely.
                if self._is_structural_cousin(best, second):
                    resolved_bindings[m.text] = best.id
                    continue

                if margin < DELTA_THRESHOLD:
                    # Too close to call deterministically
                    ambiguities.append(
                        {
                            "mention": m.text,
                            "candidates": [
                                {"id": c.id, "label": c.label, "score": c.final_score}
                                for c in candidates[:3]  # Keep top 3 for clarification
                            ],
                        }
                    )
                    continue

            # 3. Silent resolution (Margin is sufficient or only one candidate)
            resolved_bindings[m.text] = best.id

        # Determine final status
        if ambiguities:
            return {
                "status": "AMBIGUOUS",
                "resolved_bindings": resolved_bindings,
                "ambiguities": ambiguities,
                "grounding_metadata": grounding_metadata,
            }

        if missing:
            return {
                "status": "MISSING",
                "resolved_bindings": resolved_bindings,
                "missing_mentions": missing,
                "grounding_metadata": grounding_metadata,
            }

        return {
            "status": "CLEAR",
            "resolved_bindings": resolved_bindings,
            "ambiguities": [],
            "grounding_metadata": grounding_metadata,
        }

    def _is_structural_cousin(self, c1: Any, c2: Any) -> bool:
        """Check if candidates are structural cousins (Table vs its PK/ID)."""
        # Must be different kinds (Table vs Column)
        types = {c1.kind, c2.kind}
        if types != {"table", "column"}:
            return False

        # Must belong to the same table
        t1 = c1.metadata.get("table")
        t2 = c2.metadata.get("table")
        if not t1 or not t2 or t1 != t2:
            return False

        # The column must be an ID or PK
        col_cand = c1 if c1.kind == "column" else c2
        col_name = col_cand.metadata.get("column", "").lower()
        is_pk = col_cand.metadata.get("is_primary_key", False)

        return is_pk or col_name.endswith("_id") or col_name == "id"
