"""Agent state definition for LangGraph workflow."""

from typing import Annotated, Any, Dict, List, Optional, TypedDict

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

    # The resolved/contextualized query to use for retrieval/generation
    # If using history, this contains the synthesized standalone question
    active_query: Optional[str]

    # Context retrieved from RAG (Schema DDLs + Semantic Definitions)
    schema_context: str

    # Raw schema context (list of node dictionaries) for tool consumption
    raw_schema_context: Optional[List[dict]]

    # Identified table names from retrieval (used to fetch live DDL)
    table_names: Optional[List[str]]

    # The SQL query currently being generated/executed
    current_sql: Optional[str]

    # Raw result set from the database (List of dicts)
    query_result: Optional[List[dict]]

    # Result metadata from execution (truncation, columns, limits)
    result_is_truncated: Optional[bool]
    result_row_limit: Optional[int]
    result_rows_returned: Optional[int]
    result_total_row_estimate: Optional[int]
    result_columns: Optional[List[dict]]
    result_is_limited: Optional[bool]
    result_limit: Optional[int]

    # Deadline propagation and budgeting
    deadline_ts: Optional[float]
    timeout_seconds: Optional[float]

    # Schema snapshot identifier (versioning/fingerprint)
    schema_snapshot_id: Optional[str]

    # Error message from the last execution attempt (if any)
    error: Optional[str]

    # Counter to track retries and prevent infinite loops
    retry_count: int

    # Tenant identifier for multi-tenant scenarios (required for caching and RLS)
    tenant_id: Optional[int]

    # =========================================================================
    # SQL-of-Thought Planning Fields
    # =========================================================================

    # Step-by-step procedural plan for SQL generation
    procedural_plan: Optional[str]

    # JSON decomposition of query clauses (FROM, WHERE, GROUP BY, etc.)
    clause_map: Optional[dict]

    # Validated required columns/tables from schema
    schema_ingredients: Optional[List[str]]

    # =========================================================================
    # AST Validation Fields
    # =========================================================================

    # Complete AST validation result including violations
    ast_validation_result: Optional[dict]

    # Tables accessed (for audit logging and lineage tracking)
    table_lineage: Optional[List[str]]

    # Columns accessed (for compliance audits)
    column_usage: Optional[List[str]]

    # Join depth score (for complexity analysis)
    join_complexity: Optional[int]

    # =========================================================================
    # Ambiguity Resolution Fields
    # =========================================================================

    # Detected ambiguity category (schema_reference, value, temporal, metric)
    ambiguity_type: Optional[str]

    # Clarification question to present to user
    clarification_question: Optional[str]

    # User's response to clarification prompt
    user_clarification: Optional[str]

    # =========================================================================
    # Error Taxonomy Fields
    # =========================================================================

    # Targeted fix instructions from error classification
    correction_plan: Optional[str]

    # Classified error type (aggregation_misuse, missing_join, type_mismatch, etc.)
    error_category: Optional[str]

    # =========================================================================
    # Cache and Metadata
    # =========================================================================

    # Whether the current SQL was retrieved from cache
    from_cache: Optional[bool]

    # Potentially valid SQL retrieved from cache (pending validation)
    cached_sql: Optional[str]

    # Metadata associated with the cached SQL (e.g. original user query)
    cache_metadata: Optional[dict]

    # Similarity score from cache lookup (0-1 or 0-100 depending on backend)
    cache_similarity: Optional[float]

    # Context about a rejected cache hit to guide generation (e.g. "similar query but wrong entity")
    # Structure: {"sql": str, "original_query": str, "reason": str}
    rejected_cache_context: Optional[dict]

    # =========================================================================
    # Feedback and Interaction Fields
    # =========================================================================

    # Unique identifier for the current query interaction (for feedback logging)
    interaction_id: Optional[str]

    # Opaque tracing context for propagation (Serialized as Dict)
    telemetry_context: Optional[Dict[str, Any]]

    # Raw user input stored separately for debugging/telemetry
    raw_user_input: Optional[str]

    # =========================================================================
    # Visualization Fields
    # =========================================================================

    # Generated chart schema specification for the query result
    viz_spec: Optional[dict]

    # Reason for generating (or skipping) visualization
    viz_reason: Optional[str]
