"""Dataset-aware constraint pattern provider.

This module provides a single source of truth for constraint extraction patterns,
gated by the active DATASET_MODE.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from common.config.dataset import get_dataset_mode


@dataclass(frozen=True)
class ConstraintPatternSet:
    """Set of regex patterns for constraint extraction."""

    rating_patterns: List[Tuple[str, str]] = field(default_factory=list)
    entity_patterns: List[Tuple[str, str]] = field(default_factory=list)


# Rating patterns ordered by specificity (longer/more specific first)
# Legacy Pagila Patterns
_PAGILA_RATING_PATTERNS = [
    (r"\bNC[-\s]?17\b", "NC-17"),
    (r"\bPG[-\s]?13\b", "PG-13"),
    (r"\bPG\b", "PG"),
    (r"\bG[- ]?rated\b", "G"),
    (r"\brated[- ]?G\b", "G"),
    (r"\brating\s*[=:]\s*['\"]?G['\"]?\b", "G"),
    (r"\bG\b(?!\s*films?\s+rated)", "G"),
    (r"\bR\b", "R"),
]

# Legacy Entity Patterns
_PAGILA_ENTITY_PATTERNS = [
    (r"\bactors?\b", "actor"),
    (r"\bfilms?\b", "film"),
    (r"\bmovies?\b", "film"),
    (r"\brentals?\b", "rental"),
    (r"\bcustomers?\b", "customer"),
    (r"\bstores?\b", "store"),
    (r"\bpayments?\b", "payment"),
]

# Synthetic / Financial Entities (active in all modes where relevant, or just synthetic?)
# Based on existing constraint_extractor.py, these were mixed.
# For synthetic parity, we want these available in synthetic mode.
# Synthetic / Financial Entities
# TODO: Source these from artifacts/config in Phase B.2/B.3.
# For now, we avoid hardcoding per Phase B.1 requirements.
_SYNTHETIC_ENTITY_PATTERNS = []


def get_constraint_patterns(*, dataset_mode: Optional[str] = None) -> ConstraintPatternSet:
    """Get the set of constraint extraction patterns for the specified mode.

    Args:
        dataset_mode: Optional override. If None, uses common.config.get_dataset_mode().

    Returns:
        ConstraintPatternSet containing regex patterns appropriate for the mode.
    """
    if dataset_mode is None:
        try:
            dataset_mode = get_dataset_mode()
        except ValueError:
            # Fallback for safety if somehow env is messed up, though get_dataset_mode raises
            dataset_mode = "synthetic"

    if dataset_mode == "pagila":
        # In Pagila mode, we return legacy film/rating patterns
        return ConstraintPatternSet(
            rating_patterns=_PAGILA_RATING_PATTERNS,
            entity_patterns=_PAGILA_ENTITY_PATTERNS,
        )
    else:
        # Synthetic mode: No film ratings, no film/actor entities.
        return ConstraintPatternSet(
            rating_patterns=[],
            entity_patterns=_SYNTHETIC_ENTITY_PATTERNS,
        )
