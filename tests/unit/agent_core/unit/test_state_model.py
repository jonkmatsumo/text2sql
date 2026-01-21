from datetime import datetime, timezone

import pytest

from agent_core.state.domain import ConversationState, TurnRecord


def test_bounded_turn_history():
    """Verify that adding turns beyond the limit truncates the oldest ones."""
    state = ConversationState(conversation_id="bounded-test", schema_snapshot_id="snap-1")

    # Add 20 turns (assuming default limit is 15)
    for i in range(20):
        turn = TurnRecord(
            turn_id=str(i), user_nlq=f"Question {i}", timestamp=datetime.now(timezone.utc)
        )
        state.add_turn(turn)

    assert len(state.turns) == 15
    # Should keep the most recent ones (5 to 19)
    assert state.turns[0].turn_id == "5"
    assert state.turns[-1].turn_id == "19"


def test_conversation_state_roundtrip_json():
    """Verify state can be serialized to JSON and back without data loss."""
    state = ConversationState(
        conversation_id="json-test",
        schema_snapshot_id="snap-json",
        turns=[
            TurnRecord(
                turn_id="t1",
                user_nlq="Show me movies",
                timestamp=datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
                execution_status="SUCCESS",
            )
        ],
    )

    # Serialize
    json_str = state.to_json()
    assert isinstance(json_str, str)

    # Deserialize
    restored = ConversationState.from_json(json_str)

    assert restored.conversation_id == state.conversation_id
    assert len(restored.turns) == 1
    assert restored.turns[0].user_nlq == "Show me movies"
    # Verify timestamp roundtrip (checking ISO format usually handles this)
    assert restored.turns[0].timestamp == state.turns[0].timestamp


def test_schema_snapshot_required():
    """Verify schema_snapshot_id is mandatory."""
    with pytest.raises(TypeError):
        ConversationState(conversation_id="fail")


def test_state_version_increments():
    """Verify that state version bumps on modification."""
    state = ConversationState(conversation_id="ver-test", schema_snapshot_id="snap-v")
    initial_ver = state.state_version

    turn = TurnRecord(turn_id="1", user_nlq="Q", timestamp=datetime.now(timezone.utc))
    state.add_turn(turn)

    assert state.state_version > initial_ver
