"""Decision engine for ambiguity resolution.

Uses CandidateBinder to evaluate potential bindings and applies a margin-based
policy to decide whether to silently resolve or ask for clarification.
"""

import logging
import os
from typing import Any, Dict, List

from mcp_server.services.ambiguity.binder import CandidateBinder, MentionExtractor

logger = logging.getLogger(__name__)

# Configurable thresholds
DELTA_THRESHOLD = float(os.getenv("AMBIGUITY_DELTA", "0.15"))
MIN_BINDING_SCORE = float(os.getenv("AMBIGUITY_MIN_SCORE", "0.65"))


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
            return {"status": "CLEAR", "resolved_bindings": {}, "ambiguities": []}

        resolved_bindings = {}
        ambiguities = []
        missing = []

        for m in mentions:
            candidates = self.binder.get_candidates(m, schema_context)

            if not candidates:
                missing.append(m.text)
                continue

            best = candidates[0]

            # 1. Check minimum fitness
            if best.final_score < MIN_BINDING_SCORE:
                missing.append(m.text)
                continue

            # 2. Check margin for ambiguity
            if len(candidates) > 1:
                second = candidates[1]
                margin = best.final_score - second.final_score

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
            }

        if missing:
            return {
                "status": "MISSING",
                "resolved_bindings": resolved_bindings,
                "missing_mentions": missing,
            }

        return {"status": "CLEAR", "resolved_bindings": resolved_bindings, "ambiguities": []}
