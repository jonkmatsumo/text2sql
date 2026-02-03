# Control Plane Portability (Feasibility: SQLite + DuckDB)

## Scope (Docs Only)

This document evaluates feasibility of a **non-Postgres control plane**
for SQLite and DuckDB. No code changes are proposed here.

## SQLite

**Pros**
- Embedded, zero-ops deployment for dev/test.
- Easy local setup and CI friendliness.

**Risks / Gaps**
- Concurrency limitations (write locks under load).
- Limited JSON/JSONB capabilities vs Postgres JSONB semantics.
- Missing advanced indexing features relied upon by some queries.
- Migration tooling assumes Postgres capabilities in several places.

**Open Questions**
- Do we need concurrent writes in control plane for production?
- Can we accept reduced JSON querying features?
- How do we validate migrations for SQLite parity?

## DuckDB

**Pros**
- Fast analytics engine; good read performance.
- Embedded deployment option.

**Risks / Gaps**
- Limited transactional concurrency for write-heavy workloads.
- JSON feature parity varies; Postgres JSONB semantics are not equivalent.
- Ecosystem / operational tooling not as mature for control-plane usage.

**Open Questions**
- Is control-plane write volume low enough for DuckDB?
- Do we need fine-grained access controls / row-level policies?
- How would migrations and schema evolution be validated?

## Recommendation (Feasibility Only)

- SQLite/DuckDB are **promising for dev/test** control planes.
- Production control plane portability is **high risk** without a deeper
  audit of JSON, indexing, migration tooling, and concurrency needs.
