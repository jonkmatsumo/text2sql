# Observability Metrics Definitions

This document defines the derived metrics computed from raw OpenTelemetry traces for the Text-to-SQL agent.

## Core Metrics

### 1. End-to-End Latency
- **Definition**: The total time elapsed for a single trace (interaction).
- **Formula**: `max(span.end_time) - min(span.start_time)` for all spans in a trace.
- **Unit**: Milliseconds (ms).
- **Storage**: `otel.trace_metrics.duration_ms`.

### 2. Error Rate
- **Definition**: The percentage of traces that resulted in a logical error or system failure.
- **Signal**: A trace is considered "errored" if:
    - Any span has `status_code="ERROR"`.
    - (Future) The root span has specific error attributes.
- **Storage**: `otel.trace_metrics.has_error` (boolean).

### 3. Stage-Level Metrics (Per Trace)
We decompose a trace into logical stages based on the span naming convention or `span.kind`.

| Stage Name | Trigger / Span Name Pattern | Description |
| :--- | :--- | :--- |
| `router` | `router_node` | Latency of the routing decision logic. |
| `retrieval` | `retrieve_context_node` | Time spent fetching schema/context from RAG. |
| `generation` | `generate_sql_node` | Time spent waiting for LLM SQL generation. |
| `execution` | `validate_and_execute_node` | Time spent executing SQL against the target DB. |
| `synthesis` | `synthesize_insight_node` | Time spent generating the final text response. |

- **Storage**: `otel.stage_metrics` table with `(trace_id, stage, duration_ms)`.

## Future / "Not Available" Metrics

### Time to First Token (TTFT)
- **Status**: **Not Available** (v1).
- **Rationale**: Currently, the LLM spans capture total duration. We do not yet reliably capture specific "first token" timestamps from the provider key in standard attributes.
- **Todo**: Requires upstream changes to `langchain-mcp-adapters` or custom callbacks to record TTFT as a span event.

### Estimated Cost ($)
- **Status**: **Not Available** (v1).
- **Rationale**: Token usage is occasionally present in `llm_usage` attributes, but not consistently across all models/providers in the current OTEL export.
- **Todo**: Standardize `llm.token_count.total`, `llm.token_count.prompt`, `llm.token_count.completion` attributes on all generic/generation spans.
