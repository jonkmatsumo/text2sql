# Text2SQL System Investigation Report

**Date:** 2026-01-28
**Investigator:** Senior Staff Engineer (Read-Only Audit)
**Scope:** Correctness, reliability, security, operability, and long-term maintainability

---

## Phase 0: System Inventory

| Service | Language | Responsibility | Port | Data Stores |
|---------|----------|----------------|------|-------------|
| **postgres-db** | PostgreSQL 16 | Query-target database (business data) | 5432 | pgvector |
| **agent-control-db** | PostgreSQL 16 | Control-plane (metadata, cache, policies) | 5433 | pgvector |
| **memgraph** | Cypher | Graph database for schema relationships | 7687 | In-memory graph |
| **minio** | S3 API | Object storage for trace blobs | 9000 | S3-compatible |
| **mcp-server** | Python/FastMCP | Tool server (SSE transport) | 8000 | postgres-db, agent-control-db, memgraph |
| **agent-service** | Python/FastAPI | HTTP API for LangGraph agent | 8081 | mcp-server |
| **ui-api-gateway** | Python/FastAPI | REST wrapper for MCP tools | 8082 | mcp-server |
| **streamlit** | Python/Streamlit | Legacy admin UI | 8501 | mcp-server, agent-control-db |
| **ui** | TypeScript/React | Modern React UI | 3005 | agent-service, ui-api-gateway, otel-worker |
| **otel-collector** | OTEL Contrib | Trace collection and forwarding | 4317/4318 | - |
| **otel-worker** | Python/FastAPI | Trace processor and dual-write sink | 4320 | PostgreSQL, MinIO |
| **tempo** | Grafana Tempo | Trace backend (optional) | 3200 | - |
| **grafana** | Grafana | Dashboards and analytics | 3000 | tempo, otel-worker |

**Background Jobs:**
- OTEL Worker ingestion queue processor (async batch processing)
- Airflow scheduler/worker (evaluation DAGs)
- Enrichment pipeline (LLM-based schema descriptions)

---

## Executive Summary (Top 10 Critical Risks)

1. **P0 - No Authentication/Authorization:** All API endpoints accept user-supplied `tenant_id` without verification. Any client can access any tenant's data by changing the parameter.

2. **P0 - Missing LLM Error Handling:** All 6 LLM invocation sites in agent nodes lack try/catch blocks. LLM timeouts, rate limits, or auth failures crash the entire agent run.

3. **P0 - Hardcoded Credentials in Git:** `docker-compose.app.yml` contains `DB_PASS: secure_agent_pass` and similar defaults. These are committed to version control.

4. **P1 - Broken Distributed Tracing:** No trace context propagation between services. Agent→MCP and Agent→OTEL Worker calls don't inject W3C traceparent headers.

5. **P1 - Race Condition in Alias Cache:** `CanonicalAliasService._cache` can be corrupted during concurrent reload operations. No synchronization mechanism exists.

6. **P1 - Conversation State Lost Updates:** No optimistic locking on `conversation_states` table. Concurrent updates silently overwrite each other.

7. **P1 - Cache Lookup Path Untested:** The `cache_lookup_node()` function and its routing logic have zero unit tests despite being the entry point to the workflow.

8. **P1 - HTTP 200 on Errors:** UI API Gateway returns HTTP 200 even when MCP tools fail. Clients cannot distinguish success from failure via status code.

9. **P2 - No Python Type Checking:** Neither pre-commit hooks nor CI enforce mypy/pyright. Type errors are only caught at runtime.

10. **P2 - API Contract Drift:** Multiple field mismatches between UI TypeScript types and backend responses (e.g., `signature_key` vs `id`, `sql` vs `sql_query`).

---

## Phase 1: Ranked Issue Table

### P0 - Critical (Immediate Production Risk)

| Area | Issue | Evidence | Blast Radius | Why It Matters Now |
|------|-------|----------|--------------|-------------------|
| **Security** | No authentication layer | `src/ui_api_gateway/app.py:145-292` - all admin endpoints unprotected | System-wide | Any client can approve/reject interactions, modify pin rules, publish examples |
| **Security** | Tenant ID is user-supplied | `src/agent_service/app.py:34` - `tenant_id: int = Field(default=1)` | System-wide | Horizontal privilege escalation possible - users can access other tenants' data |
| **Security** | Hardcoded credentials | `docker-compose.app.yml:17,50,100` - `DB_PASS: secure_agent_pass` | System-wide | Secrets in git history, all deployments use same credentials |
| **Correctness** | LLM calls unprotected | `src/agent/nodes/generate.py:350`, `plan.py:153`, `correct.py:133`, `synthesize.py:87`, `router.py:163` | Service-level | Rate limits, timeouts, auth failures crash agent with unhandled exception |

### P1 - High (Significant Operational Risk)

| Area | Issue | Evidence | Blast Radius | Why It Matters Now |
|------|-------|----------|--------------|-------------------|
| **Observability** | No trace context propagation | `src/agent/tools.py:79-82` - MCP calls don't inject headers | System-wide | Cannot trace requests across service boundaries; debugging multi-service issues impossible |
| **Concurrency** | Alias cache race condition | `src/mcp_server/services/canonicalization/alias_service.py:42-59` | Service-level | Concurrent requests during reload get empty/partial aliases |
| **Concurrency** | Conversation state overwrites | `data/database/control-plane/05-conversation-states.sql` - no version check | Local | Multi-turn conversations can lose state when concurrent updates occur |
| **Testing** | Cache lookup untested | No test file for `src/agent/nodes/cache_lookup.py` | Local | Cache hit/miss paths completely unvalidated; latency optimization could silently break |
| **Testing** | Multi-tenant isolation untested | Zero tests for tenant_id segregation in cache/schema | System-wide | Cross-tenant data leakage would go undetected |
| **API** | HTTP 200 on errors | `src/ui_api_gateway/app.py:98-106` - `_call_tool` returns error dict with 200 | Service-level | UI cannot use HTTP status to detect failures; all errors require response body parsing |
| **Error Handling** | Errors not set on spans | Only 3 `span.set_status()` calls in codebase | System-wide | OTEL traces show OK status even when errors occur; error rates underreported |
| **Observability** | MCP tools not traced | MCP tool handlers have no child spans | Service-level | execute_sql_query latency invisible in traces; can't identify slow tools |

### P2 - Medium (Technical Debt / Future Risk)

| Area | Issue | Evidence | Blast Radius | Why It Matters Now |
|------|-------|----------|--------------|-------------------|
| **Build** | No Python type checking | `.pre-commit-config.yaml` missing mypy; no CI type check | Service-level | Type errors discovered at runtime; refactoring is high-risk |
| **Build** | No Dockerfile linting | `.pre-commit-config.yaml:comment` - "handled in CI" but not present | Local | Dockerfile best practices not enforced |
| **API** | Contract drift | `src/mcp_server/tools/admin/list_approved_examples.py:23-32` returns `signature_key`; UI expects `id` | Local | UI components may fail to manage examples correctly |
| **API** | No API versioning | Only OTEL Worker has `/api/v1/` prefix | System-wide | Breaking changes cannot be managed; all clients must update simultaneously |
| **Testing** | Integration tests manual only | `.github/workflows/integration.yml` - `workflow_dispatch` only | Service-level | Breaking changes to service interactions not caught on PR |
| **Error Handling** | Silent error swallowing | `src/agent/nodes/retrieve.py:100-114`, `generate.py:235-237` | Local | Failures logged but not propagated; degraded quality not visible |
| **UI** | Race condition in URL sync | `ui/src/routes/TraceSearch.tsx:289-322` | Local | Filter changes can desync with URL during navigation |
| **UI** | Stale data without refresh | `ui/src/routes/MetricsPreview.tsx:165-190` - loads once, no refresh | Local | Metrics dashboard shows outdated data with no indication |
| **Concurrency** | DAL factory singleton race | `src/dal/factory.py:69-80` - no double-checked locking | Service-level | Multiple store instances possible during concurrent startup |
| **Database** | No migration rollback tests | Control-plane uses raw SQL, no alembic | Service-level | Failed migrations cannot be safely rolled back |

---

## Phase 2: Dependency & Coupling Risks

### High-Risk Coupling Points

1. **Agent → MCP Server Tight Coupling**
   - `src/agent/tools.py` hardcodes MCP tool names
   - Tool schema changes require agent code updates
   - No contract tests between services
   - **Risk:** MCP tool signature change breaks agent silently

2. **UI → Multiple Backend Services**
   - `ui/src/api.ts` calls 3 different backends (agent-service, ui-api-gateway, otel-worker)
   - Different error handling patterns per service
   - No shared client library
   - **Risk:** API changes require coordinated updates across services

3. **Control Plane Database → All Services**
   - `agent-control-db` accessed by: mcp-server, agent-service, ui-api-gateway, streamlit
   - Schema changes require coordinated migrations
   - No versioned access layer
   - **Risk:** Schema migration breaks multiple services simultaneously

4. **LangGraph State → Conversation Persistence**
   - `src/agent/graph.py:335` hardcodes `schema_snapshot_id: "v1.0"`
   - State format changes break persisted conversations
   - No state migration strategy
   - **Risk:** Agent updates invalidate all active conversations

5. **Embedding Model → Cache + Recommendations**
   - Same embedding model used for cache lookup and recommendations
   - Model change requires full re-embedding
   - `src/dal/memgraph/graph_store.py:263-267` - HNSW disabled due to missing module
   - **Risk:** Embedding model upgrade causes stale cache hits

---

## Operational Red Flags

### Issues Most Likely to Cause Production Incidents

| Issue | Scenario | Detection Difficulty | Recovery Complexity |
|-------|----------|---------------------|---------------------|
| **Alias cache corruption** | High traffic during pattern reload | Hard (silent wrong answers) | Restart service |
| **LLM rate limit crash** | Traffic spike exceeds OpenAI limits | Easy (500 errors) | Wait for limit reset |
| **Conversation state lost** | Two browser tabs editing same thread | Hard (data appears normal) | Manual state reconstruction |
| **Tenant data leak** | Attacker guesses tenant_id | Hard (no audit trail) | Data breach notification |
| **Trace chain broken** | Debugging multi-service issue | Easy (incomplete traces) | Manual log correlation |
| **Cache hit with wrong SQL** | Canonicalization drift | Hard (query returns results) | Flush cache, investigate |
| **Migration failure** | New control-plane column | Medium (startup failure) | No rollback path |

### Monitoring Gaps

- No alert for cache hit ratio degradation
- No alert for LLM error rate spike
- No alert for cross-tenant access attempts
- No alert for conversation state conflicts
- Trace error rates underreported (span status not set)

---

## Explicit Non-Findings

### Areas Investigated and Determined Not Problematic

| Area | Investigation | Finding |
|------|---------------|---------|
| **SQL Injection** | Reviewed `src/agent/validation/ast_validator.py`, `policy_enforcer.py`, `tenant_rewriter.py` | **Strong protection.** AST-based validation blocks dangerous patterns, parameterized queries used, read-only user for execution |
| **RLS Enforcement** | Reviewed `data/database/control-plane/06-row-policies.sql`, `07-multi-tenancy.sql`, `src/dal/database.py:225-254` | **Dual-layer enforcement.** Database policies + application-level tenant rewriting both active |
| **Connection Pool Leaks** | Reviewed `src/dal/database.py:45-51`, `control_plane.py:59-65` | **Proper cleanup.** asyncpg pools with context manager cleanup, transactions properly scoped |
| **OTEL Worker Durability** | Reviewed `src/otel_worker/ingestion/processor.py` | **Good resilience.** Retry logic with exponential backoff, graceful shutdown flushes buffer, per-item failure tracking |
| **Python Dependency Pinning** | Reviewed `uv.lock` (4,645 lines) | **Reproducible builds.** Lock file used everywhere with `uv sync --frozen` |
| **Vector Index Concurrency** | Reviewed `src/ingestion/vector_indexes/thread_safe.py` | **Well-designed.** Double-buffering pattern with minimal lock contention |
| **Enrichment Pipeline** | Reviewed `src/ingestion/enrichment/main.py` | **Properly bounded.** Semaphore(5) limits concurrent LLM calls, WAL for durability |

---

## Stubs and Incomplete Features

| Location | Description | Status |
|----------|-------------|--------|
| `src/mcp_server/services/ops/maintenance.py:150` | `yield "Cache re-indexed (STUB)."` | Placeholder - not functional |
| `src/mcp_server/services/ops/maintenance.py:131-144` | `hydrate_schema()` marked "Phase 1 stub" | Placeholder - not functional |
| `src/ui/pages/3_System_Operations.py:65,73` | "Hydrate Schema" and "Re-index Cache" buttons disabled | "Coming soon" |
| `src/mcp_server/services/cache/service.py:115-116` | `get_cache_stats()` returns placeholder | Stats not implemented |
| `src/dal/memgraph/graph_store.py:263-267` | HNSW vector search commented out | "vector_search module unavailable" |
| `src/agent/graph.py:335,338` | `schema_snapshot_id: "v1.0"`, `prompt_version: "v1.0"` | Hardcoded - TODO for dynamic versioning |

---

## Summary Statistics

| Category | P0 | P1 | P2 | Total |
|----------|----|----|----|----|
| Security | 3 | 0 | 0 | 3 |
| Correctness | 1 | 1 | 2 | 4 |
| Observability | 0 | 3 | 0 | 3 |
| Testing | 0 | 2 | 1 | 3 |
| Concurrency | 0 | 2 | 1 | 3 |
| API/Contracts | 0 | 1 | 2 | 3 |
| Build/CI | 0 | 0 | 3 | 3 |
| UI | 0 | 0 | 2 | 2 |
| **Total** | **4** | **9** | **11** | **24** |

---

## Recommended Prioritization

### Immediate (This Sprint)
1. Add try/catch to all LLM invocations (P0)
2. Remove hardcoded credentials from docker-compose (P0)
3. Add synchronization to CanonicalAliasService (P1)

### Short-Term (Next 2 Sprints)
4. Implement authentication layer for API endpoints (P0)
5. Add trace context propagation between services (P1)
6. Add optimistic locking to conversation_states (P1)
7. Write tests for cache_lookup_node (P1)
8. Return appropriate HTTP status codes from ui-api-gateway (P1)

### Medium-Term (Next Quarter)
9. Add Python type checking to CI (P2)
10. Implement API versioning strategy (P2)
11. Enable integration tests on PR (P2)
12. Resolve API contract drift issues (P2)

---

*End of Investigation Report*
