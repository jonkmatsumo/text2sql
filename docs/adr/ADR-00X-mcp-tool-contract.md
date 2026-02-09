# ADR-00X: MCP Tool Contract

## Status
Accepted

## Context
As the number of MCP tools grows, we need a stable contract to ensure security (tenant isolation), reliability (input validation), and observability (tracing).

## Decision
All MCP tools MUST adhere to the following contract:

### 1. Registry Enforcement
- Every tool MUST be registered in `src/mcp_server/tools/registry.py`.
- The registration helper MUST wrap every tool with `trace_tool(name)`.

### 2. Validation
- **Tenant Isolation**: Any tool that queries or modifies data MUST accept a `tenant_id` and call `require_tenant_id(tenant_id, TOOL_NAME)`.
- **Input Bounding**: Tools with a `limit` parameter MUST call `validate_limit(limit, TOOL_NAME)`.
- Validation errors MUST be returned as a standard error envelope.

### 3. Response Structure
- Tools MUST return a JSON-serialized `ToolResponseEnvelope` (or `ExecuteSQLQueryResponseEnvelope` for SQL execution).
- Successful responses place the payload in the `result` field.
- Errors MUST be populated in the `error` field using the `ErrorMetadata` model.

### 4. Observability
- All tool execution MUST be traced via OpenTelemetry.
- Spans MUST include tool name, tenant ID (if applicable), and response size.

### 5. Admin Gating
- Administrative or destructive tools MUST be gated by the `MCP_ENABLE_ADMIN_TOOLS` environment variable.

## Consequences
- Consistent error handling in the agent.
- Stronger security guarantees through mandatory tenant validation.
- Easier debugging with unified tracing.
