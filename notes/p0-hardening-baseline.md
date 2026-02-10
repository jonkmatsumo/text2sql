# P0 Hardening Baseline (2026-02-10)

## Unit test baseline

Command:

```bash
.venv/bin/python -m pytest -q
```

Observed status:

- 6 failed
- 1835 passed
- 362 skipped

Current pre-existing failures (outside this P0 scope):

1. `tests/unit/dal/test_cockroach_no_transaction.py::test_cockroach_uses_postgres_pool_path`
2. `tests/unit/dal/test_duckdb_query_target.py::test_duckdb_query_target_introspection_and_exec`
3. `tests/unit/otel_worker/test_metrics_aggregation.py::TestMetricsPreview::test_get_metrics_preview_empty`
4. `tests/unit/otel_worker/test_metrics_aggregation.py::TestMetricsPreview::test_get_metrics_preview_logic`
5. `tests/unit/otel_worker/test_trace_aggregations.py::TestTraceAggregations::test_compute_trace_aggregations`
6. `tests/unit/otel_worker/test_trace_list.py::TestTraceList::test_duration_filters_included_in_query`

## `tool_error_response` definitions and uses

Definitions:

- `src/mcp_server/utils/errors.py`
- `src/mcp_server/utils/envelopes.py`

Known tool caller importing from duplicate helper module:

- `src/mcp_server/tools/feedback/submit_feedback.py`

Current direct invocations found:

- `src/mcp_server/tools/feedback/submit_feedback.py`
- `src/mcp_server/utils/validation.py`

## Raw exception reflection targets (`str(e)`) in MCP tools

Primary P0 audit examples and neighboring hotspots:

- `src/mcp_server/tools/get_table_schema.py`
- `src/mcp_server/tools/search_relevant_tables.py`
- `src/mcp_server/tools/update_cache.py`
- `src/mcp_server/tools/manage_pin_rules.py`
- `src/mcp_server/tools/admin/export_approved_to_fewshot.py`
- `src/mcp_server/tools/admin/hydrate_schema.py`
- `src/mcp_server/tools/admin/reindex_cache.py`
- `src/mcp_server/tools/admin/generate_patterns.py`
- `src/mcp_server/tools/resolve_ambiguity.py`
- `src/mcp_server/tools/feedback/submit_feedback.py`
- `src/mcp_server/tools/get_semantic_subgraph.py`

## Optional tenant signatures currently requiring P0 review

Tools with optional `tenant_id` signatures identified:

- `src/mcp_server/tools/list_tables.py`
- `src/mcp_server/tools/get_table_schema.py`
- `src/mcp_server/tools/get_semantic_definitions.py`
- `src/mcp_server/tools/search_relevant_tables.py`
- `src/mcp_server/tools/admin/list_approved_examples.py`
- `src/mcp_server/tools/get_sample_data.py` (already validates tenant via `require_tenant_id`)

## Tenant default values identified

- Agent entrypoint default: `src/agent/graph.py` (`run_agent_with_tracing(... tenant_id: int = 1 ...)`)
- RAG retrieval default: `src/mcp_server/services/rag/retrieval.py` (`tenant_id: int = 1`)
