from datetime import datetime, timezone

import pytest

from agent.state.classifier import TopicShiftDetector, TurnType, classify_turn
from agent.state.domain import ConversationState, ExecutionContext, TurnRecord


class MockSimilarity:
    """Stub for embedding similarity."""

    def __init__(self, score=0.9):
        """Initialize with a fixed score."""
        self.score = score

    def calculate(self, txt1, txt2):
        """Return the pre-configured score."""
        return self.score


@pytest.fixture
def empty_state():
    """Create a pristine ConversationState."""
    return ConversationState(conversation_id="test", schema_snapshot_id="snap")


@pytest.fixture
def active_state():
    """Create a state with active execution context."""
    state = ConversationState(conversation_id="test", schema_snapshot_id="snap")
    # Simulate a previous execution
    state.execution_context = ExecutionContext(
        last_sql="SELECT * FROM movies",
        last_tables_used=["film", "inventory"],
    )
    # Add a previous turn so similarity comparison works
    state.turns.append(
        TurnRecord(turn_id="1", user_nlq="show me movies", timestamp=datetime.now(timezone.utc))
    )
    # Simulate an active working intent
    # (In real usage, this would be populated)
    return state


def test_classify_reset_phrases(active_state):
    """Verify that reset phrases trigger a RESET turn type."""
    assert classify_turn("start over", active_state).turn_type == TurnType.RESET
    assert classify_turn("clear chat", active_state).turn_type == TurnType.RESET
    assert classify_turn("new question", active_state).turn_type == TurnType.RESET


def test_classify_refinement_markers(active_state):
    """Verify refinement markers on an active state."""
    assert classify_turn("add a filter for rating", active_state).turn_type == TurnType.REFINEMENT
    assert classify_turn("group by category", active_state).turn_type == TurnType.REFINEMENT
    assert classify_turn("sort by release year", active_state).turn_type == TurnType.REFINEMENT
    assert classify_turn("only last 30 days", active_state).turn_type == TurnType.REFINEMENT


def test_classify_followup_markers(active_state):
    """Verify follow-up markers that imply digging into results."""
    assert (
        classify_turn("drill into action movies", active_state).turn_type
        == TurnType.FOLLOW_UP_ON_RESULTS
    )
    assert (
        classify_turn("show me the rows for that", active_state).turn_type
        == TurnType.FOLLOW_UP_ON_RESULTS
    )


def test_classify_repair_markers(active_state):
    """Verify repair/correction markers."""
    assert classify_turn("no that's wrong", active_state).turn_type == TurnType.REPAIR
    assert classify_turn("schema mismatch", active_state).turn_type == TurnType.REPAIR


def test_classify_new_question_empty_state(empty_state):
    """If state is empty, almost anything is a NEW_QUESTION."""
    assert classify_turn("show me top movies", empty_state).turn_type == TurnType.NEW_QUESTION


def test_classify_topic_shift_similarity(active_state):
    """Verify that low similarity triggers NEW_QUESTION despite active state."""
    # Low similarity = topic shift
    detector = TopicShiftDetector(similarity_provider=MockSimilarity(score=0.2))
    result = classify_turn("how many customers in Texas", active_state, shift_detector=detector)
    assert result.turn_type == TurnType.NEW_QUESTION
    assert "Topic shift" in result.reason


def test_classify_topic_shift_high_similarity(active_state):
    """Verify that high similarity keeps it as REFINEMENT (or unspecified if ambiguous)."""
    # High similarity = related
    detector = TopicShiftDetector(similarity_provider=MockSimilarity(score=0.9))
    # Without explicit refinement markers, it might default to REFINEMENT if highly similar
    # Or strict logic might say "ambiguous". For now, let's assume similarity implies refinement
    # if no other strong signal exists, or at least NOT a new question.
    # Actually, a bare query "show me actors" with 0.9 similarity to "show me movies" is tricky.
    # Let's use a clear refinement that *also* has high similarity.
    result = classify_turn("filter by PG-13", active_state, shift_detector=detector)
    assert result.turn_type == TurnType.REFINEMENT


def test_refinement_precedence_over_similarity(active_state):
    """
    Explicit keywords should override low similarity.

    Example: 'filter by...' on a totally different topic should be a REFINEMENT.
    """
    # Even if similarity is low, "add a filter" is structurally a refinement.
    detector = TopicShiftDetector(similarity_provider=MockSimilarity(score=0.1))
    result = classify_turn(
        "add a filter for unrelated thing",
        active_state,
        shift_detector=detector,
    )
    assert result.turn_type == TurnType.REFINEMENT
