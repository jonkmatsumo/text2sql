# DAL Query Tracing Contract

## Scope
Applies to query-target execution only. No SQL text is recorded.

## Spans
- `dal.query.execute` (sync providers)
- `dal.query.submit` (async providers)
- `dal.query.poll` (async providers)
- `dal.query.fetch` (async providers)

## Required Attributes
- `db.provider` (canonical provider id)
- `db.execution_model` (`sync`/`async`)
- `db.status` (`ok`/`error`)
- `db.duration_ms`
- `db.statement_hash` (SHA256 of SQL text; no raw SQL)

## Non-Goals
- No SQL text recording
- No automatic retries or rewrites
- No span creation unless explicitly enabled (`DAL_TRACE_QUERIES=true`)
