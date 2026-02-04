"""Unit tests for AgentState TypedDict."""

# Import mock classes from conftest (set up before this module imports)
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from agent.state import AgentState


class TestAgentStateStructure:
    """Test AgentState structure and instantiation."""

    def test_state_instantiation(self):
        """Test creating state with all fields."""
        messages = [HumanMessage(content="Show me all customers")]
        state = AgentState(
            messages=messages,
            schema_context="Table: customer. Columns: id, name",
            current_sql="SELECT * FROM customer",
            query_result=[{"id": 1, "name": "John"}],
            error=None,
            retry_count=0,
        )

        assert len(state["messages"]) == 1
        assert state["schema_context"] == "Table: customer. Columns: id, name"
        assert state["current_sql"] == "SELECT * FROM customer"
        assert state["query_result"] == [{"id": 1, "name": "John"}]
        assert state["error"] is None
        assert state["retry_count"] == 0

    def test_state_with_optional_none(self):
        """Test with optional fields as None."""
        messages = [HumanMessage(content="test")]
        state = AgentState(
            messages=messages,
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        assert state["current_sql"] is None
        assert state["query_result"] is None
        assert state["error"] is None
        assert state["retry_count"] == 0

    def test_state_type_validation(self):
        """Test TypedDict type checking."""
        messages = [HumanMessage(content="test")]
        state = AgentState(
            messages=messages,
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        # Verify all required fields are present
        assert "messages" in state
        assert "schema_context" in state
        assert "current_sql" in state
        assert "query_result" in state
        assert "error" in state
        assert "retry_count" in state

        # Verify types
        assert isinstance(state["messages"], list)
        assert isinstance(state["schema_context"], str)
        assert isinstance(state["retry_count"], int)

    def test_state_required_fields(self):
        """Verify all required fields are present."""
        messages = [HumanMessage(content="test")]
        state = AgentState(
            messages=messages,
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        required_fields = [
            "messages",
            "schema_context",
            "current_sql",
            "query_result",
            "error",
            "retry_count",
        ]
        for field in required_fields:
            assert field in state, f"Required field '{field}' missing"

    def test_state_metadata_fields_present(self):
        """Verify result/timeout/schema metadata keys exist on AgentState."""
        metadata_fields = [
            "result_is_truncated",
            "result_row_limit",
            "result_rows_returned",
            "result_total_row_estimate",
            "result_columns",
            "deadline_ts",
            "timeout_seconds",
            "schema_snapshot_id",
        ]
        for field in metadata_fields:
            assert field in AgentState.__annotations__, f"Missing AgentState key: {field}"


class TestMessageReducer:
    """Test message reducer functionality."""

    def test_add_messages_reducer(self):
        """Test message history persistence with reducer."""
        initial_messages = [HumanMessage(content="Show me customers")]
        state = AgentState(
            messages=initial_messages,
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        # Simulate adding a new message (reducer behavior)
        new_message = AIMessage(content="I'll query the customer table")
        # In actual LangGraph, the reducer handles this automatically
        # Here we test that messages can be updated
        updated_messages = state["messages"] + [new_message]
        state["messages"] = updated_messages

        assert len(state["messages"]) == 2
        assert isinstance(state["messages"][0], HumanMessage)
        assert isinstance(state["messages"][1], AIMessage)

    def test_message_list_append(self):
        """Test adding messages to history."""
        messages = [HumanMessage(content="query 1")]
        state = AgentState(
            messages=messages,
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        # Add multiple messages
        state["messages"].append(AIMessage(content="response 1"))
        state["messages"].append(ToolMessage(content="tool result", tool_call_id="1"))

        assert len(state["messages"]) == 3
        assert isinstance(state["messages"][0], HumanMessage)
        assert isinstance(state["messages"][1], AIMessage)
        assert isinstance(state["messages"][2], ToolMessage)

    def test_empty_messages(self):
        """Test state with empty message list."""
        state = AgentState(
            messages=[],
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        assert len(state["messages"]) == 0
        assert isinstance(state["messages"], list)


class TestStateUpdates:
    """Test state field updates."""

    def test_state_schema_context_update(self):
        """Test updating schema_context."""
        messages = [HumanMessage(content="test")]
        state = AgentState(
            messages=messages,
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        new_context = "Table: customer. Columns: id, name, email"
        state["schema_context"] = new_context

        assert state["schema_context"] == new_context

    def test_state_sql_update(self):
        """Test updating current_sql."""
        messages = [HumanMessage(content="test")]
        state = AgentState(
            messages=messages,
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        new_sql = "SELECT id, name FROM customer WHERE id = 1"
        state["current_sql"] = new_sql

        assert state["current_sql"] == new_sql

    def test_state_result_update(self):
        """Test updating query_result."""
        messages = [HumanMessage(content="test")]
        state = AgentState(
            messages=messages,
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        new_result = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
        state["query_result"] = new_result

        assert state["query_result"] == new_result
        assert len(state["query_result"]) == 2

    def test_state_error_update(self):
        """Test updating error."""
        messages = [HumanMessage(content="test")]
        state = AgentState(
            messages=messages,
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        error_msg = "Column 'invalid_column' does not exist"
        state["error"] = error_msg

        assert state["error"] == error_msg

    def test_state_retry_count_increment(self):
        """Test retry_count increment."""
        messages = [HumanMessage(content="test")]
        state = AgentState(
            messages=messages,
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        # Simulate retry
        state["retry_count"] = state["retry_count"] + 1
        assert state["retry_count"] == 1

        state["retry_count"] = state["retry_count"] + 1
        assert state["retry_count"] == 2


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_state_all_none_optional(self):
        """Test all optional fields as None."""
        messages = [HumanMessage(content="test")]
        state = AgentState(
            messages=messages,
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        assert state["current_sql"] is None
        assert state["query_result"] is None
        assert state["error"] is None

    def test_state_empty_strings(self):
        """Test with empty string values."""
        messages = [HumanMessage(content="test")]
        state = AgentState(
            messages=messages,
            schema_context="",
            current_sql="",
            error="",
            query_result=None,
            retry_count=0,
        )

        assert state["schema_context"] == ""
        assert state["current_sql"] == ""
        assert state["error"] == ""

    def test_state_large_message_list(self):
        """Test with many messages."""
        messages = [HumanMessage(content=f"query {i}") for i in range(100)]
        state = AgentState(
            messages=messages,
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        assert len(state["messages"]) == 100
        assert all(isinstance(msg, HumanMessage) for msg in state["messages"])

    def test_state_with_tool_messages(self):
        """Test state with various message types."""
        messages = [
            HumanMessage(content="Show me customers"),
            AIMessage(content="I'll query the customer table"),
            ToolMessage(content='[{"id": 1, "name": "John"}]', tool_call_id="1"),
        ]
        state = AgentState(
            messages=messages,
            schema_context="",
            current_sql=None,
            query_result=None,
            error=None,
            retry_count=0,
        )

        assert len(state["messages"]) == 3
        assert isinstance(state["messages"][0], HumanMessage)
        assert isinstance(state["messages"][1], AIMessage)
        assert isinstance(state["messages"][2], ToolMessage)

    def test_state_with_complex_query_result(self):
        """Test state with complex nested query results."""
        messages = [HumanMessage(content="test")]
        complex_result = [
            {
                "id": 1,
                "name": "John",
                "orders": [{"order_id": 1, "total": 100.50}],
            },
            {
                "id": 2,
                "name": "Jane",
                "orders": [{"order_id": 2, "total": 200.75}],
            },
        ]
        state = AgentState(
            messages=messages,
            schema_context="",
            current_sql=None,
            query_result=complex_result,
            error=None,
            retry_count=0,
        )

        assert len(state["query_result"]) == 2
        assert "orders" in state["query_result"][0]
        assert isinstance(state["query_result"][0]["orders"], list)
