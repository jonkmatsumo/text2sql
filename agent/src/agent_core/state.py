"""Agent state definition for LangGraph workflow."""

from typing import Annotated, List, Optional, TypedDict

from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages


class AgentState(TypedDict):
    """
    State structure for the Text 2 SQL agent workflow.

    This state persists across all nodes in the LangGraph, maintaining
    conversation history, context, and execution results.
    """

    # Full conversation history (User, AI, Tool messages)
    # Uses add_messages reducer to handle history persistence
    messages: Annotated[List[BaseMessage], add_messages]

    # Context retrieved from RAG (Schema DDLs + Semantic Definitions)
    schema_context: str

    # The SQL query currently being generated/executed
    current_sql: Optional[str]

    # Raw result set from the database (List of dicts)
    query_result: Optional[List[dict]]

    # Error message from the last execution attempt (if any)
    error: Optional[str]

    # Counter to track retries and prevent infinite loops
    retry_count: int

    # Tenant identifier for multi-tenant scenarios (required for caching and RLS)
    tenant_id: Optional[int]
