# Text2SQL Hardening Investigation Follow-ups (Jan 2026)

This document captures the baseline risks identified during the investigation and establishes reproduction paths for verification.

## 1. SQL Safety (P0)
**Risk:** Read-only enforcement in the MCP server relies on a list of regex forbidden patterns.
**File:** `src/mcp_server/tools/execute_sql_query.py`
**Reproduction:**
- Use comments to hide keywords: `SELECT 1; -- DROP TABLE users;` (Currently caught by regex if keyword exists anywhere)
- Use nested subqueries or CTEs: `WITH t AS (DELETE FROM users RETURNING *) SELECT * FROM t;` (Caught because `DELETE` is in list)
- Bypass with `SET`: `SET session_replication_role = 'replica';` (NOT caught, `SET` not in list)
- Bypass with `COPY`: `COPY (SELECT * FROM users) TO '/tmp/data.csv';` (NOT caught, `COPY` not in list)
- Bypassing word boundaries: `SELECT 1; /*! DROP */ TABLE users;` (Depends on regex engine)

## 2. Telemetry Ingestion Fragility (P1)
**Risk:** No ingestion buffer; spans dropped under load.
**File:** `src/otel_worker/app.py`
**Current Behavior:** `receive_traces` calls `enqueue_ingestion` which writes to Postgres. If Postgres is slow, the thread pool can be exhausted.
**Evidence:** `src/otel_worker/storage/postgres.py` implements `enqueue_ingestion` as a direct synchronous write to DB via `asyncio.to_thread`.

## 3. Metrics Integrity (P1)
**Risk:** Metrics Preview aggregates only latest 500 traces client-side.
**File:** `ui/src/routes/MetricsPreview.tsx`
**Evidence:**
```typescript
        const result = await listTraces({
          limit: 500,
          order: "desc"
        });
```

## 4. System Ops Stubs (P2)
**Risk:** "Coming soon" placeholders for critical operations.
**File:** `ui/src/routes/SystemOperations.tsx`
**Evidence:** Tabs for `schema` and `cache` contain only "Coming Soon" text.

## 5. API Contract Fragility (P2)
**Risk:** Manual duplication of types.
**File:** `ui/src/types.ts`
**Evidence:** Matches Pydantic models in `otel_worker` and `ui_api_gateway` but is maintained manually.

## 6. Internal Security Posture (P1)
**Risk:** Service-to-service traffic is unauthenticated.
**File:** `src/ui_api_gateway/app.py`
**Evidence:** No `X-Internal-Token` or similar validation in `ApproveInteractionRequest` or other sensitive endpoints.
