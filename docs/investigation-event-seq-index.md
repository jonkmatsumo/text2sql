# Investigation: event.seq as Indexed Column

**Date:** 2026-01-17
**Status:** Investigation Only (No Implementation)
**Related:** [trace-inspection-ux-grafana.md](./trace-inspection-ux-grafana.md)

---

## Current State

The `event.seq` attribute is:
- **Set during instrumentation** in the agent's telemetry module
- **Stored in span_attributes JSON** during OTLP ingestion
- **Extracted via SQL**: `span_attributes->>'event.seq'`

## Reliability Through OTLP Parsing

| Factor | Status | Notes |
|--------|--------|-------|
| OTLP → Postgres persistence | ✅ Works | Standard attribute flow |
| Older traces | ⚠️ Missing | Pre-instrumentation traces lack event.seq |
| Type consistency | ⚠️ String | Stored as stringified integer in JSON |

## Pros of Promoting to Indexed Column

1. **Query Performance**: Direct column access vs JSON extraction
2. **Index Support**: B-tree index for efficient sorting
3. **Type Safety**: Integer column ensures consistent sorting
4. **Query Simplicity**: `ORDER BY event_seq` vs `ORDER BY (span_attributes->>'event.seq')::int`

## Cons of Promoting to Indexed Column

1. **Migration Complexity**: Backfill required for existing traces
2. **NULL Handling**: Old traces will have NULL values
3. **Schema Coupling**: Ties schema to application-level attribute
4. **Storage Overhead**: Additional column vs JSON storage

## Suggested Migration Shape (If Implemented)

```python
# Option 1: GENERATED column (read-only, auto-computed)
def upgrade():
    op.add_column(
        "spans",
        sa.Column(
            "event_seq",
            sa.Integer(),
            sa.Computed("(span_attributes->>'event.seq')::int"),
            nullable=True,
        ),
        schema="otel",
    )
    op.create_index(
        "ix_otel_spans_trace_id_event_seq",
        "spans",
        ["trace_id", "event_seq"],
        schema="otel",
    )

# Option 2: Real column with backfill
def upgrade():
    op.add_column(
        "spans",
        sa.Column("event_seq", sa.Integer(), nullable=True),
        schema="otel",
    )
    # Backfill existing data
    op.execute("""
        UPDATE otel.spans
        SET event_seq = (span_attributes->>'event.seq')::int
        WHERE span_attributes->>'event.seq' IS NOT NULL
    """)
    op.create_index(
        "ix_otel_spans_trace_id_event_seq",
        "spans",
        ["trace_id", "event_seq"],
        schema="otel",
    )
```

## Recommendation

**Defer until proven necessary.** Current JSON extraction approach is acceptable for single-trace queries. Consider promotion if:

1. Query performance becomes a bottleneck in Grafana dashboards
2. `event.seq` usage becomes widespread and consistent
3. Application requires deterministic ordering guarantees

---

## Related Work

- Dashboard already uses `ORDER BY (span_attributes->>'event.seq')::int ASC NULLS LAST`
- Current approach handles missing values gracefully
- GIN index on span_attributes NOT recommended per project constraints
