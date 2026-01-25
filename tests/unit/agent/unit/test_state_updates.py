from datetime import datetime, timedelta, timezone

import pytest

from agent.state.classifier import TurnType
from agent.state.domain import ConversationState, ExecutionContext, WorkingIntent
from agent.state.manager import StateManager


@pytest.fixture
def manager():
    """Create test manager."""
    return StateManager()


@pytest.fixture
def active_state():
    """Create test state."""
    state = ConversationState(
        conversation_id="test-update",
        schema_snapshot_id="snap-1",
        working_intent=WorkingIntent(filters=[{"col": "year", "op": "eq", "val": 2023}]),
        execution_context=ExecutionContext(last_sql="SELECT * FROM movies", last_success=True),
    )
    return state


def test_reset_clears_active_context(manager, active_state):
    """RESET should clear working_intent and execution_context."""
    updated = manager.update_state_pre_retrieval(
        active_state, "start over", TurnType.RESET, datetime.now(timezone.utc)
    )
    assert updated.working_intent is None
    assert updated.execution_context is None
    # Meta persists
    assert updated.conversation_id == "test-update"


def test_new_question_starts_fresh_intent(manager, active_state):
    """NEW_QUESTION should clear old intent and start a fresh one."""
    updated = manager.update_state_pre_retrieval(
        active_state, "how many actors?", TurnType.NEW_QUESTION, datetime.now(timezone.utc)
    )
    # Context cleared
    assert updated.execution_context is None
    # New empty intent created (waiting to be populated by extraction)
    assert updated.working_intent is not None
    assert updated.working_intent.filters == []


def test_refinement_preserves_intent(manager, active_state):
    """REFINEMENT should keep the existing intent (to be patched later)."""
    updated = manager.update_state_pre_retrieval(
        active_state, "and restrict to PG-13", TurnType.REFINEMENT, datetime.now(timezone.utc)
    )
    assert updated.working_intent is not None
    # Existing filter should still be there (until the extractor modifies it)
    # Note: extraction updates are not tested here.
    # The manager's job pre-retrieval is just to NOT clear it.
    assert len(updated.working_intent.filters) == 1


def test_followup_inherits_context(manager, active_state):
    """FOLLOW_UP_ON_RESULTS requires keeping the execution context."""
    updated = manager.update_state_pre_retrieval(
        active_state,
        "drill into that",
        TurnType.FOLLOW_UP_ON_RESULTS,
        datetime.now(timezone.utc),
    )
    assert updated.execution_context is not None
    assert updated.execution_context.last_sql == "SELECT * FROM movies"


def test_inactivity_ttl_clears_context(manager, active_state):
    """If TTL expired, treat as hard reset even if TurnType says Refinement (conceptually)."""
    # Force last active to be old
    long_ago = datetime.now(timezone.utc) - timedelta(minutes=61)
    active_state.last_active_at = long_ago

    # Manager handles TTL check implicitly or explicitly
    updated = manager.check_ttl(active_state, datetime.now(timezone.utc), ttl_minutes=60)

    assert updated.working_intent is None
    assert updated.execution_context is None


def test_post_execution_update(manager, active_state):
    """Verify that post-execution updates history and context."""
    now = datetime.now(timezone.utc)
    updated = manager.update_state_post_execution(
        state=active_state,
        user_nlq="how many actors?",
        sql="SELECT count(*) FROM actors",
        execution_status="SUCCESS",
        result_summary="Count: 100",
        result_schema={"cols": ["count"]},
        rowcount=100,
        tables_used=["actor"],
        timestamp=now,
    )

    # Check execution context updated
    assert updated.execution_context.last_sql == "SELECT count(*) FROM actors"
    assert updated.execution_context.last_rowcount == 100
    assert updated.execution_context.last_success is True

    # Check turn added to history
    assert len(updated.turns) == 1
    last_turn = updated.turns[-1]
    assert last_turn.assistant_sql == "SELECT count(*) FROM actors"
    assert last_turn.execution_status == "SUCCESS"
