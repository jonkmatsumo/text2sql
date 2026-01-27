"""Agent Service for Streamlit app using HTTP backends."""

import logging
import os
from typing import Dict, Optional

import httpx

logger = logging.getLogger(__name__)

AGENT_SERVICE_URL = os.getenv("AGENT_SERVICE_URL", "http://localhost:8081")
UI_API_URL = os.getenv("UI_API_URL", "http://localhost:8082")


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
        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{AGENT_SERVICE_URL}/agent/run",
                json={
                    "question": question,
                    "tenant_id": tenant_id,
                    "thread_id": thread_id,
                },
            )
            response.raise_for_status()
            return response.json()

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

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(
                    f"{UI_API_URL}/feedback",
                    json={
                        "interaction_id": interaction_id,
                        "thumb": thumb,
                        "comment": comment,
                    },
                )
                response.raise_for_status()
                return True
        except Exception as e:
            print(f"Error submitting feedback: {e}")

        return False
