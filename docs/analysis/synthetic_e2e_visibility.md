# Synthetic Data E2E Visibility & Safety Analysis

## Overview
This document analyzes the end-to-end observability and safety mechanisms for the synthetic data cutover. It verifies that critical decision points—Clarification, Intent Inference, and Policy Enforcement—are adequately instrumented to support debugging and audit compliance.

## Visibility Posture

### 1. Intent Inference (Router Node)
*   **File:** `agent/src/agent_core/nodes/router.py`
*   **Status:** ✅ Fully Observable
*   **Instrumentation:**
    *   **Logic:** Uses `resolve_ambiguity` MCP tool deterministic checks.
    *   **Telemetry:**
        *   `resolution_status`: CLEAR, AMBIGUOUS, or MISSING.
        *   `ambiguity_data`: Full JSON dump of reasoning, options, and missing data logic.
        *   `resolved_bindings`: JSON dump of any pre-resolved entity bindings.
        *   `action`: "clarify", "refuse", or "plan".
    *   **Audit Logic:** If ambiguity is detected, the detailed reasoning is preserved in the trace, allowing verification of why a query was flagged.

### 2. Clarification Loop (Clarify Node)
*   **File:** `agent/src/agent_core/nodes/clarify.py`
*   **Status:** ✅ Fully Observable
*   **Instrumentation:**
    *   **Logic:** Uses `LangGraph.interrupt` to pause for human feedback.
    *   **Telemetry:**
        *   `clarification_question`: The question asked to the user.
        *   `user_response`: The raw text response from the user (truncated to 1000 chars).
        *   `response_length`: Size of the user input.
    *   **Audit Logic:** Both the system's question and the user's answer are captured, providing a complete audit trail of the resolution.

### 3. Syntax & Schema Safety (Validate Node)
*   **File:** `agent/src/agent_core/nodes/validate.py`
*   **Status:** ✅ Fully Observable
*   **Instrumentation:**
    *   **Logic:** Uses `sqlglot` AST analysis (`ast_validator.py`).
    *   **Telemetry:**
        *   `is_valid`: Boolean status.
        *   `violation_count`: Number of security/logic issues.
        *   `violations`: Detailed list of specific errors (e.g., `restriced_table`, `forbidden_command`).
        *   `table_lineage`: List of accessed tables.
        *   `join_complexity`: Number of joins.
    *   **Audit Logic:** Every generated SQL is vetted BEFORE execution. Metadata extraction ensures we know exactly which tables are accessed.

### 4. Runtime Policy Enforcement (Execute Node)
*   **File:** `agent/src/agent_core/nodes/execute.py`
*   **Status:** ✅ Fully Observable
*   **Instrumentation:**
    *   **Logic:**
        *   **Start:** Re-validates SQL with `PolicyEnforcer` (Double-Check).
        *   **Rewrite:** Uses `TenantRewriter` (AST-based) to inject RLS predicates.
    *   **Telemetry:**
        *   `rewritten_sql`: The final, safe SQL actually executed on the DB.
        *   `original_sql`: The raw LLM generation.
        *   `error`: Captures blocking reasons (e.g., "Blocked unsafe SQL").
    *   **Logging:**
        *   Emits "SQL Audit" INFO logs containing `{original_sql, rewritten_sql, tenant_id, event="runtime_policy_enforcement"}`.
    *   **Audit Logic:** The transformation from raw to safe SQL is explicitly traced.

## Conclusion
The system demonstrates high-fidelity observability across the entire query lifecycle. No major blind spots were identified after the Phase 4 instrumentation updates.
