from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class Mention:
    """A segment of the user query that may refer to a schema element."""

    text: str
    type: str  # 'noun_phrase', 'entity', 'metric', 'filter'
    start_char: int
    end_char: int
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Candidate:
    """A potential schema element that a mention could bind to."""

    kind: str  # 'table', 'column', 'value'
    id: str  # e.g., 'film.rating'
    label: str
    scores: Dict[str, float] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def final_score(self) -> float:
        """Compute weighted final score.

        If ontology_match is present and == 1.0, return 1.0 immediately (short-circuit).
        Otherwise use standard weighted scoring.
        """
        # Ontology match is a short-circuit: if present and 1.0, it's a definitive match
        if self.scores.get("ontology_match") == 1.0:
            return 1.0

        # NOTE: Semantic scoring was removed because the binder was using
        # FastEmbed (384-dim) while schema nodes use OpenAI (1536-dim),
        # causing incomparable similarity scores. Ontology match and lexical
        # scoring provide more reliable grounding signals.
        weights = {"lexical": 0.7, "relational": 0.3}
        return sum(self.scores.get(k, 0.0) * weights.get(k, 0.0) for k in weights)
