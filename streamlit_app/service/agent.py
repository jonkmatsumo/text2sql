"""Agent Service for Streamlit app.

This service handles agent integration, state management, and result processing.
It encapsulates business logic previously in app_logic.py.
"""

import sys
from pathlib import Path
from typing import Dict, Optional

# Add agent to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "agent" / "src"))


class AgentService:
    """Service for interacting with the Text2SQL Agent."""

    @staticmethod
    async def run_agent(question: str, tenant_id: int, thread_id: str = None) -> Dict:
        """
        Run the agent workflow and return structured results.

        Args:
            question: Natural language question from the user
            tenant_id: Tenant identifier for multi-tenant scenarios
            thread_id: Unique identifier for the conversation thread

        Returns:
            Dictionary containing:
                - sql: Generated SQL query (if any)
                - result: Query results as list of dicts (if successful)
                - response: Natural language response (if any)
                - error: Error message (if any)
                - from_cache: Boolean indicating if SQL came from cache
                - interaction_id: Unique identifier for the interaction

        Raises:
            Exception: If agent workflow fails unexpectedly
        """
        from agent_core.graph import run_agent_with_tracing

        # Execute with full tracing and logging
        state = await run_agent_with_tracing(
            question=question, tenant_id=tenant_id, thread_id=thread_id
        )

        results = {
            "sql": state.get("current_sql"),
            "result": state.get("query_result"),
            "response": None,
            "error": state.get("error"),
            "from_cache": state.get("from_cache", False),
            "interaction_id": state.get("interaction_id"),
            "viz_spec": state.get("viz_spec"),
            "viz_reason": state.get("viz_reason"),
        }

        # Extract response from messages
        if state.get("messages"):
            results["response"] = state["messages"][-1].content

        # Handle clarification
        if state.get("clarification_question"):
            results["response"] = state["clarification_question"]

        return results

    @staticmethod
    def format_conversation_entry(question: str, results: Dict) -> Dict:
        """
        Format a conversation entry for storage/display.

        Args:
            question: User's question
            results: Results from run_agent()

        Returns:
            Formatted conversation entry dictionary
        """
        return {
            "question": question,
            "sql": results.get("sql"),
            "result": results.get("result"),
            "response": results.get("response"),
            "error": results.get("error"),
            "from_cache": results.get("from_cache", False),
            "interaction_id": results.get("interaction_id"),
            "viz_spec": results.get("viz_spec"),
            "viz_reason": results.get("viz_reason"),
        }

    @staticmethod
    def validate_tenant_id(tenant_id: Optional[int]) -> int:
        """
        Validate and return tenant ID, defaulting to 1 if None.

        Args:
            tenant_id: Optional tenant ID

        Returns:
            Valid tenant ID (defaults to 1)
        """
        return tenant_id if tenant_id is not None and tenant_id > 0 else 1

    @staticmethod
    async def submit_feedback(interaction_id: str, thumb: str, comment: str = None) -> bool:
        """Submit feedback for an interaction."""
        if not interaction_id:
            return False

        from agent_core.tools import get_mcp_tools

        try:
            tools = await get_mcp_tools()
            feedback_tool = next((t for t in tools if t.name == "submit_feedback"), None)
            if feedback_tool:
                await feedback_tool.ainvoke(
                    {"interaction_id": interaction_id, "thumb": thumb, "comment": comment}
                )
                return True
        except Exception as e:
            print(f"Error submitting feedback: {e}")

        return False
