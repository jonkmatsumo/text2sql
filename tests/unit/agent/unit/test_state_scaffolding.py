from agent.state.domain import ConversationState


def test_smoke_conversation_state_instantiation():
    """Verify we can import and instantiate a basic state."""
    state = ConversationState(conversation_id="123", schema_snapshot_id="snap-001")
    assert state.conversation_id == "123"
    assert state.schema_snapshot_id == "snap-001"


def test_default_values_exist():
    """Verify that optional fields have sane defaults."""
    state = ConversationState(conversation_id="abc", schema_snapshot_id="snap-002")
    assert state.turns == []
    assert state.working_intent is None
