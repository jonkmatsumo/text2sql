# Policy Enforcement Responsibilities

This document clarifies the security and policy enforcement responsibilities between the **Agent** (upstream caller) and the **MCP Server** (downstream executor).

## Conceptual Boundary

*   **Agent (Intent)**: The Agent is responsible for formulating the *intent* of the user. It decides "what to do" based on user prompts. It is trusted to invoke tools but must provide valid justification.
*   **MCP Server (Safety)**: The MCP Server is responsible for *safety* and *correctness*. It enforces strict boundaries on "how it is done" to prevent data exfiltration, resource exhaustion, or dangerous operations.

## Responsibility Matrix

| Feature | Responsibility | Mechanism |
| :--- | :--- | :--- |
| **Authentication** | Agent / Gateway | Pass tenant/user context via headers or arguments. |
| **SQL Generation** | Agent | Generates SQL based on schema. |
| **SQL Validation** | **MCP Server** | Enforces read-only (SELECT), prevents DDL/DML, validates AST against dialect. |
| **Row Limits** | **MCP Server** | Enforces hard limits (e.g., 1000 rows) regardless of LIMIT clause in SQL. |
| **Query Timeout** | **MCP Server** | Enforces execution time limits (e.g., 30s) via database driver settings. |
| **Telemetry** | Shared | Agent starts trace; MCP MUST create spans for all tool executions. |

## Telemetry Enforcement

The MCP Server enforces observability requirements:
*   **Configuration**: `TELEMETRY_ENFORCEMENT_MODE` (warn/error).
*   **Requirement**: Every tool execution must be wrapped in an OpenTelemetry span (`mcp.tool.<name>`).
*   **Validation**: The `trace_tool` decorator automatically creates these spans. If tracing fails or is missing, the server logs a warning or raises an error based on configuration.

## Upstream Assumptions

Handlers in `src/mcp_server/tools` assume:
1.  **Authorized Context**: The caller has already authenticated the user.
2.  **Valid Intent**: The operation requested matches the user's goal (though parameters are validated).
3.  **Trace Context**: The caller propagates trace context (W3C headers) so that tool spans are linked to the parent request trace.
