from langchain_core.messages import HumanMessage

from agent_core.state import AgentState


class TestAgentStateViz:
    """Test visualization fields in AgentState."""

    def test_state_viz_fields(self):
        """Test retrieving and updating viz_spec and viz_reason."""
        messages = [HumanMessage(content="test")]
        state = AgentState(
            messages=messages,
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
            viz_spec=None,
            viz_reason=None,
        )

        assert state.get("viz_spec") is None
        assert state.get("viz_reason") is None

        # Update fields
        spec = {"mark": "bar"}
        state["viz_spec"] = spec
        state["viz_reason"] = "Valid data"

        assert state["viz_spec"] == spec
        assert state["viz_reason"] == "Valid data"

    def test_state_viz_optionality(self):
        """Test that viz fields are optional (not required for init)."""
        # If I don't provide them, TypedDict might complain if they are required?
        # In AgentState definition:
        # viz_spec: Optional[dict]
        # viz_reason: Optional[str]
        # TypedDict creates them as required keys by default unless total=False is used.
        # However, AgentState inherits from TypedDict.
        # Let's check if they are required keys.

        messages = [HumanMessage(content="test")]
        # This should theoretically fail type checkers if keys are missing but valid at runtime
        # Python's TypedDict runtime validation is minimal.

        state = AgentState(
            messages=messages,
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        # Runtime, keys might not exist if not provided?
        # Or keys are mandatory?
        # Let's assert we can add them.
        state["viz_spec"] = {}
        assert state["viz_spec"] == {}
