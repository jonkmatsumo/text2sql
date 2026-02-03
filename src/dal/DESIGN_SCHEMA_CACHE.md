# Design: Materialized Schema Cache (Read-Through)

## Summary
Provide an **in-memory, read-through cache** for schema introspection results. This cache
is **introspection-only** and **does not mutate** provider state.

## Cache Keys
- Provider name (canonical)
- Database identifier (if available)
- Schema name
- Table name (for table-level calls)
- Method name (list tables / get table def / sample rows)

## TTL Strategy
- Fixed TTL in seconds (configurable)
- Default: 300 seconds
- No background refresh (read-through only)

## Size Limits / LRU
- Configurable max entries via `DAL_SCHEMA_CACHE_MAX_ENTRIES`
- Default: 1000 entries
- LRU eviction runs after inserts (expired entries are pruned first)

## Manual Invalidation
- Clear all
- Clear by provider
- Clear by (provider, schema)
- Clear by (provider, schema, table)

## Guardrails / Non-Goals
- **No schema mutation or normalization**
- **No cross-provider cache sharing**
- **No automatic invalidation on DDL**
- **No persistence (in-memory only)**

## Gating
- Requires `DAL_EXPERIMENTAL_FEATURES=true`
- Requires explicit capability flag: `supports_schema_cache=true`

## Notes
This cache exists to reduce repeated introspection calls, not to hide provider
semantics. All cached values are raw provider outputs wrapped in canonical
DAL models.
