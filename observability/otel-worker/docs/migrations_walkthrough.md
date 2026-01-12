# Hardened OTEL Schema Management Walkthrough

This task introduces formal, versioned schema migrations for the OTEL worker, replacing the previous unsafe runtime table creation with a robust Alembic-driven pipeline.

## 1. Migration Tooling
Alembic has been initialized in `observability/otel-worker/`.

- **Migration Path**: `observability/otel-worker/migrations/versions/`
- **Environment Driven**: The database connection is loaded from the `POSTGRES_URL` environment variable.
- **Dynamic Schema**: The target schema (default `otel`) is configurable via `OTEL_DB_SCHEMA`.

## 2. Hardened Schema Design
The schema has been aligned with the proposed JSON-based OTEL storage model.

### `otel.traces`
- Renamed `start_ts`/`end_ts` to `start_time`/`end_time`.
- Added `resource_attributes` (JSONB) and `trace_attributes` (JSONB).
- Preserved existing auditing columns (`environment`, `tenant_id`, etc.).

### `otel.spans`
- Renamed `start_ts`/`end_ts` to `start_time`/`end_time`.
- Renamed `status` to `status_code` and `attributes` to `span_attributes`.
- Added `status_message` and `created_at`.

## 3. Backward Compatibility
The baseline migration (`163d8f446eb9`) includes **adoption logic**:
1. If the `otel` schema or tables are missing, it creates them.
2. If tables exist from the previous runtime generation, it **renames columns** and **adds new ones** in-place, preserving all existing data.

## 4. Operational Safeties
- **Fail-Fast Startup**: The `init_db()` function in `postgres.py` no longer creates tables. Instead, it validates the schema exists and raises a `RuntimeError` if migrations are missing.
- **Optimized Indexing**: Added btree indexes on `trace_id`, `span_id`, and time-descending composite indexes for service/latency queries.

## 5. Deployment Instructions

### Local Development
To apply migrations locally:
```bash
cd observability/otel-worker
export POSTGRES_URL="postgresql://user:pass@localhost:5432/dbname"
../../venv/bin/alembic upgrade head
```

### Rollback
To revert the last migration:
```bash
../../venv/bin/alembic rollback -1
```

### Verification
Run the new migration verification suite:
```bash
cd observability/otel-worker
export PYTHONPATH=$PYTHONPATH:$(pwd)/src
../../venv/bin/python tests/test_migrations.py
```
*(Note: requires a live Postgres connection defined in .env)*
