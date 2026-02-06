"""Dataset-aware constraint pattern provider.

This module provides a single source of truth for constraint extraction patterns,
gated by the active DATASET_MODE.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Tuple


@dataclass(frozen=True)
class ConstraintPatternSet:
    """Set of regex patterns for constraint extraction."""

    rating_patterns: List[Tuple[str, str]] = field(default_factory=list)
    entity_patterns: List[Tuple[str, str]] = field(default_factory=list)


# Synthetic / Financial Entities
_SYNTHETIC_ENTITY_PATTERNS = [
    (r"\bmerchants?\b", "merchant"),
    (r"\baccounts?\b", "account"),
    (r"\btransactions?\b", "transaction"),
    (r"\bbanks?\b", "institution"),  # For 'Find banks' test case
]


def get_constraint_patterns(*, dataset_mode: Optional[str] = None) -> ConstraintPatternSet:
    """Get the set of constraint extraction patterns.

    Args:
        dataset_mode: Deprecated/Ignored. Kept for signature compatibility.

    Returns:
        ConstraintPatternSet containing regex patterns for financial domain.
    """
    # Synthetic mode: No film ratings, no film/actor entities.
    return ConstraintPatternSet(
        rating_patterns=[],
        entity_patterns=_SYNTHETIC_ENTITY_PATTERNS,
    )
