# Quickstart: Debugging Traces

This guide shows how to investigate slow or failing traces using the observability tools.

## Prerequisites

- OTEL Worker running at `http://localhost:4320`
- Grafana running at `http://localhost:3001`

## Step 1: Find the Trace ID

### From Admin UI

1. Go to **Admin Panel** → **Recent Interactions**
2. Click **Review** on an interaction
3. Look for the **Trace Observability** section showing the trace ID

### From API Response

The trace ID is returned in agent response headers or can be extracted from logs.

## Step 2: View in Grafana

1. Open the [Trace Detail Dashboard](http://localhost:3001/d/text2sql-trace-detail)
2. Enter the trace ID in the **Trace ID** variable box
3. Review:
   - **Trace Summary**: Overall duration, span count, error count
   - **Span Sequence**: Ordered list of all spans with timing

## Step 3: Query OTEL APIs

### Get Trace Summary

```bash
curl http://localhost:4320/api/v1/traces/{trace_id}
```

### Get All Spans with Attributes

```bash
curl "http://localhost:4320/api/v1/traces/{trace_id}/spans?include=attributes"
```

### Get Raw OTLP Blob

```bash
curl http://localhost:4320/api/v1/traces/{trace_id}/raw
```

## Step 4: Use debug_trace.py

For a quick visual tree view:

```bash
# Basic usage
python scripts/debug_trace.py --trace-id {trace_id}

# With custom API URL
python scripts/debug_trace.py --trace-id {trace_id} --api-url http://otel-worker:4320

# Output raw JSON
python scripts/debug_trace.py --trace-id {trace_id} --json
```

### Example Output

```
============================================================
Trace: abc123def456
Spans: 12
Start: 2026-01-17T10:00:00+00:00
============================================================

✓ text2sql_agent [seq:1] +0ms [1523ms]
  ✓ router_node [seq:2] +5ms [45ms]
  ✓ retrieval_node [seq:3] +50ms [320ms]
    ✓ mcp_tool_call (tool_call)[seq:4] +55ms [280ms]
  ✓ generate_sql_node [seq:5] +380ms [890ms]
    ✓ llm_call [seq:6] +385ms [850ms]
  ✓ execution_node [seq:7] +1280ms [120ms]
```

## Common Issues

### Trace Not Found

- Verify trace_id format (32 hex characters)
- Check if trace was ingested (may take a few seconds)
- Verify OTEL Worker is running and accessible

### Missing event.seq

Older traces may not have `event.seq` attributes. Spans will still be ordered by `start_time`.

### Large Response Payloads

For large LLM responses, use the OTEL API to inspect full `span_attributes` rather than inline Grafana display.
