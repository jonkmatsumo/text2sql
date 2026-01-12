# OTEL Worker Runtime Schema Analysis

## Current Schema (Runtime Generated)
The OTEL worker currently uses `init_db()` in `postgres.py` with `CREATE TABLE IF NOT EXISTS` to manage its schema.

### Tables

#### `otel.traces`
| Column | Type | Notes |
|--------|------|-------|
| `trace_id` | TEXT | Primary Key |
| `start_ts` | TIMESTAMPTZ | Start of the first span |
| `end_ts` | TIMESTAMPTZ | End of the last span |
| `duration_ms` | BIGINT | end - start |
| `service_name` | TEXT | Extracted from resource attributes |
| `environment` | TEXT | Environment variable |
| `tenant_id` | TEXT | Extracted app attribute |
| `interaction_id` | TEXT | Extracted app attribute |
| `status` | TEXT | "OK" or "ERROR" |
| `error_count` | INT | Count of spans with error status |
| `span_count` | INT | Total spans in trace batch |
| `raw_blob_url` | TEXT | Pointer to MinIO blob |
| `created_at` | TIMESTAMPTZ | DEFAULT `now()` |

#### `otel.spans`
| Column | Type | Notes |
|--------|------|-------|
| `span_id` | TEXT | Primary Key |
| `trace_id` | TEXT | FK to `otel.traces` |
| `parent_span_id`| TEXT | Nullable |
| `name` | TEXT | Span name |
| `kind` | TEXT | Span kind |
| `start_ts` | TIMESTAMPTZ | |
| `end_ts` | TIMESTAMPTZ | |
| `duration_ms` | BIGINT | |
| `status` | TEXT | OTEL status code |
| `attributes` | JSONB | Raw span attributes |
| `events` | JSONB | Raw span events |

## Migration Target Schema Gaps

| Entity | Current | Proposed | Compatibility Action |
|--------|---------|----------|----------------------|
| Trace | `start_ts`, `end_ts` | `start_time`, `end_time` | Rename columns in migration |
| Trace | (missing) | `resource_attributes` | Add column |
| Trace | (missing) | `trace_attributes` | Add column |
| Span | `start_ts`, `end_ts` | `start_time`, `end_time` | Rename columns in migration |
| Span | `status` | `status_code` | Rename column |
| Span | (missing) | `status_message` | Add column |
| Span | `attributes` | `span_attributes` | Rename column |
| Span | (missing) | `created_at` | Add column with `now()` |

## Risk Assessment
- **Data Preservation**: Renaming columns is non-destructive in Postgres.
- **Existing Logic**: `save_trace_and_spans` will need immediate updates after migrations are applied to match new column names.
- **Backwards Compatibility**: Migration 001 will create the schema/tables if missing or adopt existing ones if they match.

## Phase 1 Deliverable Summary
- Documentation completed.
- Gaps identified.
- Decision: Use `ALTER TABLE ... RENAME` to align with the proposed schema while preserving data.
