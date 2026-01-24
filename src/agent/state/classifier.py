from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Optional

from agent.state.domain import ConversationState


class TurnType(Enum):
    """Enumeration of possible conversation turn types."""

    NEW_QUESTION = auto()
    REFINEMENT = auto()
    FOLLOW_UP_ON_RESULTS = auto()
    REPAIR = auto()
    RESET = auto()


@dataclass
class ClassificationResult:
    """Result object containing the classification decision and reasoning."""

    turn_type: TurnType
    reason: str


class TopicShiftDetector:
    """Detects if a new utterance shifts the topic significantly from the previous one."""

    def __init__(self, similarity_provider: Any = None):
        """Initialize with an optional similarity provider (e.g. embedding model)."""
        self.similarity_provider = similarity_provider
        self.threshold = 0.5

    def is_shift(self, current_nlq: str, previous_nlq: Optional[str]) -> bool:
        """Calculate similarity score and determine if it falls below threshold."""
        if not previous_nlq or not self.similarity_provider:
            return False  # Default to no shift if we can't tell

        score = self.similarity_provider.calculate(current_nlq, previous_nlq)
        return score < self.threshold


def classify_turn(
    nlq: str,
    state: ConversationState,
    shift_detector: Optional[TopicShiftDetector] = None,
) -> ClassificationResult:
    """Classify the incoming natural language query into a TurnType."""
    normalized = nlq.lower().strip()

    # 1. Check for explicit RESET
    if any(
        phrase in normalized for phrase in ["start over", "clear chat", "new question", "reset"]
    ):
        return ClassificationResult(TurnType.RESET, "Explicit reset phrase detected")

    # 2. If no active context (no execution context), it's a NEW_QUESTION
    if not state.execution_context:
        return ClassificationResult(TurnType.NEW_QUESTION, "No active context found")

    # 3. Check for REPAIR markers
    if any(
        phrase in normalized
        for phrase in ["that's wrong", "incorrect", "schema mismatch", "not right"]
    ):
        return ClassificationResult(TurnType.REPAIR, "Repair marker detected")

    # 4. Check for FOLLOW_UP_ON_RESULTS markers
    if any(
        phrase in normalized
        for phrase in ["drill into", "show me the rows", "details for", "behind that"]
    ):
        return ClassificationResult(TurnType.FOLLOW_UP_ON_RESULTS, "Drill-down marker detected")

    # 5. Check for REFINEMENT markers
    refinement_markers = [
        "filter",
        "group by",
        "sort by",
        "order by",
        "only",
        "exclude",
        "limit",
        "top",
        "bottom",
        "add",
    ]
    if any(marker in normalized for marker in refinement_markers):
        return ClassificationResult(TurnType.REFINEMENT, "Refinement keyword detected")

    # 6. Check for Topic Shift (if detector provided)
    if shift_detector:
        # Get the last user NLQ from turns
        last_turn_nlq = state.turns[-1].user_nlq if state.turns else None
        if shift_detector.is_shift(normalized, last_turn_nlq):
            return ClassificationResult(TurnType.NEW_QUESTION, "Topic shift detected by similarity")

    # Default fallback: If context exists but no markers found,
    # it's usually safer to assume REFINEMENT (e.g. "what about sending it to Alice?")
    # OR NEW_QUESTION.
    # For this exercise, let's default to REFINEMENT if it's ambiguous but we have context,
    # as "NEW_QUESTION" is usually a distinct break.
    return ClassificationResult(
        TurnType.REFINEMENT, "Defaulting to refinement based on active context"
    )
