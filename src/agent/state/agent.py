"""Agent state definition for LangGraph workflow."""

from typing import Annotated, Any, Dict, List, Optional, TypedDict

from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages

from agent.models.termination import TerminationReason


class AgentState(TypedDict):
    """
    State structure for the Text 2 SQL agent workflow.

    This state persists across all nodes in the LangGraph, maintaining
    conversation history, context, and execution results.
    """

    # Full conversation history (User, AI, Tool messages)
    # Uses add_messages reducer to handle history persistence
    messages: Annotated[List[BaseMessage], add_messages]

    # Unique identifier for this specific run execution
    run_id: str

    # Pinned policy snapshot for deterministic validation
    policy_snapshot: Optional[Dict[str, Any]]

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
    result_completeness: Optional[dict]
    result_cap_detected: Optional[bool]
    result_cap_mitigation_applied: Optional[bool]
    result_cap_mitigation_mode: Optional[str]
    result_capability_required: Optional[str]
    result_capability_supported: Optional[bool]
    result_fallback_policy: Optional[str]
    result_fallback_applied: Optional[bool]
    result_fallback_mode: Optional[str]
    result_auto_paginated: Optional[bool]
    result_pages_fetched: Optional[int]
    result_auto_pagination_stopped_reason: Optional[str]
    result_prefetch_enabled: Optional[bool]
    result_prefetch_scheduled: Optional[bool]
    result_prefetch_reason: Optional[str]
    prefetch_discard_count: Optional[int]
    empty_result_guidance: Optional[str]

    # Pagination inputs for executing subsequent pages
    page_token: Optional[str]
    page_size: Optional[int]

    # Deterministic execution seed
    seed: Optional[int]

    # Deadline propagation and budgeting
    deadline_ts: Optional[float]
    timeout_seconds: Optional[float]
    interactive_session: Optional[bool]

    # Schema snapshot identifier (versioning/fingerprint)
    schema_snapshot_id: Optional[str]
    pinned_schema_snapshot_id: Optional[str]
    pending_schema_snapshot_id: Optional[str]
    pending_schema_fingerprint: Optional[str]
    pending_schema_version_ts: Optional[int]
    schema_snapshot_transition: Optional[dict]
    schema_snapshot_refresh_applied: Optional[int]
    schema_fingerprint: Optional[str]
    schema_version_ts: Optional[int]

    # Error message from the last execution attempt (if any)
    error: Optional[str]
    error_metadata: Optional[dict]
    retry_after_seconds: Optional[float]
    retry_reason: Optional[str]

    # Counter to track retries and prevent infinite loops
    retry_count: int

    # Tenant identifier for multi-tenant scenarios (required for caching and RLS)
    tenant_id: Optional[int]

    # =========================================================================
    # Budget and Safety Fields
    # =========================================================================
    # Per-request token budget configuration and consumption
    # Structure: {"max_tokens": int, "consumed_tokens": int}
    token_budget: Optional[dict]
    llm_prompt_bytes_used: Optional[int]
    llm_budget_exceeded: Optional[bool]
    llm_calls: Optional[int]
    llm_token_total: Optional[int]

    # History of error signatures in the current request to detect loops
    # Signatures are hashes of (category, normalized_message)
    error_signatures: List[str]

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
    validation_report: Optional[dict]

    # Tables accessed (for audit logging and lineage tracking)
    table_lineage: Optional[List[str]]

    # Columns accessed (for compliance audits)
    column_usage: Optional[List[str]]

    # Join depth score (for complexity analysis)
    join_complexity: Optional[int]
    query_join_count: Optional[int]
    query_estimated_table_count: Optional[int]
    query_estimated_scan_columns: Optional[int]
    query_union_count: Optional[int]
    query_detected_cartesian_flag: Optional[bool]
    query_complexity_score: Optional[int]

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

    # Schema drift hints (missing tables/columns)
    schema_drift_suspected: Optional[bool]
    missing_identifiers: Optional[List[str]]
    schema_drift_auto_refresh: Optional[bool]
    schema_refresh_count: int

    # Per-node latency tracking (seconds)
    latency_generate_seconds: Optional[float]
    latency_correct_seconds: Optional[float]
    ema_llm_latency_seconds: Optional[float]
    latency_retrieval_ms: Optional[float]
    latency_planning_ms: Optional[float]
    latency_generation_ms: Optional[float]
    latency_validation_ms: Optional[float]
    latency_execution_ms: Optional[float]
    latency_correction_loop_ms: Optional[float]

    # Retry history for debugging and telemetry
    # Structure: {"attempts": [{"reason": str, "timestamp": float}], "budget_exhausted": bool}
    retry_summary: Optional[dict]
    decision_events: Optional[List[dict]]
    decision_events_truncated: Optional[bool]
    decision_events_dropped: Optional[int]
    correction_attempts: Optional[List[dict]]
    validation_failures: Optional[List[dict]]
    correction_attempts_truncated: Optional[bool]
    validation_failures_truncated: Optional[bool]
    correction_attempts_dropped: Optional[int]
    validation_failures_dropped: Optional[int]
    retry_correction_summary: Optional[dict]
    decision_summary: Optional[dict]
    run_decision_summary: Optional[dict]

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
    cache_lookup_failed: Optional[bool]
    cache_lookup_failure_reason: Optional[str]

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

    # =========================================================================
    # Reproducibility Fields
    # =========================================================================

    # Raw output from the last tool invocation (for replay capture)
    last_tool_output: Optional[dict]

    # Replay bundle for deterministic execution
    replay_mode: Optional[bool]
    replay_bundle: Optional[dict]

    # Explicit termination reason for better observability
    termination_reason: Optional[TerminationReason]
