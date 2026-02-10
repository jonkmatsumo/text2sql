# P2 Hardening Baseline (2026-02-10)

## Retry policy defaults and routing

Current default source locations:

- `src/agent/graph.py` (`_retry_policy_mode`) defaults `AGENT_RETRY_POLICY` to `static`
- `src/agent/utils/retry.py` (`_retry_policy_mode`) defaults `AGENT_RETRY_POLICY` to `static`

Current adaptive/non-retryable decision path:

- `src/agent/graph.py` (`route_after_execution`) gates retry behavior by error category under adaptive mode
- `src/agent/utils/retry.py` (`retry_with_backoff`) uses provider classification + transient fallback

## Span contract registry and enforcement path

Current contract registry:

- `src/agent/telemetry_schema.py` (`SPAN_CONTRACTS`)

Current enforcement path:

- `src/agent/telemetry.py` (`validate_span_contract`)
- `src/agent/telemetry.py` (`_get_contract_enforce_mode`, `AGENT_TELEMETRY_CONTRACT_ENFORCE`)

Current behavior notes:

- default enforcement mode is `warn`
- critical spans are escalated from warn to error in-process via `CRITICAL_SPANS`

## MCP truncation detection in tracing

Current tracing wrapper and truncation detection:

- `src/mcp_server/utils/tracing.py` (`trace_tool`)
- `execute_sql_query` truncation is currently detected by string matching on response text

## Non-execute tool output envelope and bounding paths

Current envelope model:

- `src/common/models/tool_envelopes.py` (`GenericToolMetadata`, `ToolResponseEnvelope`)

Current bounding utilities:

- `src/mcp_server/utils/tool_output.py` (`bound_tool_output`)
- `src/mcp_server/utils/tracing.py` applies bounding for non-`execute_sql_query` tool responses

Representative non-execute tools with potentially large results:

- `src/mcp_server/tools/list_tables.py`
- `src/mcp_server/tools/get_table_schema.py`
- `src/mcp_server/tools/get_semantic_definitions.py`
- `src/mcp_server/tools/admin/get_interaction_details.py`
- `src/mcp_server/tools/admin/list_interactions.py`
- `src/mcp_server/tools/conversation/load_conversation_state.py`
