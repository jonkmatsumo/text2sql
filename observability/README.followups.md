# Observability Follow-ups & Investigation

This document tracks future improvements and investigation areas for the observability stack.

## Future Tooling Options

### 1. Jaeger
- **Pros**: Industry standard for trace visualization, powerful DAG views.
- **Cons**: Requires additional infrastructure (Cassandra/ES) for heavy loads, though simpler "all-in-one" exists.
- **Status**: Deferred. Grafana + Postgres is sufficient for v1.

### 2. SigNoz
- **Pros**: Open-source Datadog alternative, integrates metrics/logs/traces tightly.
- **Cons**: Heavier to deploy (ClickHouse).
- **Status**: Strong candidate if we need "full stack" APM later.

## Draft Issues for Backlog

### Issue A: Time to First Token (TTFT) Tracking
**Title**: feat(observability): Implement Time-to-First-Token (TTFT) metrics
**Body**:
> Currently, we only track total generation latency.
> **Requirements**:
> 1. Update `langchain-mcp-adapters` to capture the first token timestamp.
> 2. Emit a span event `llm.first_token` with the timestamp.
> 3. Update `aggregate.py` to compute `ttft_ms = event_time - span_start_time`.
> 4. Add "TTFT (p50/p90)" panel to Grafana.

### Issue B: Cost Tracking
**Title**: feat(observability): Add estimated cost tracking for LLM calls
**Body**:
> **Requirements**:
> 1. Standardize `llm_token_count` usage attributes across all providers (OpenAI, Gemini, Anthropic).
> 2. Create a `pricing_config.yaml` mapping model names to cost per 1k tokens.
> 3. Compute `cost_usd` in `aggregate.py` and store in `otel.trace_metrics`.
> 4. Add "Estimated Cost ($)" panel to Grafana.
