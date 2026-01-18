# Grounding Pipeline Architecture

This document describes the canonicalization and grounding pipeline that ensures
user queries are properly resolved to schema elements before SQL generation.

## Pipeline Order

The grounding pipeline follows this strict order:

```
User Query
    │
    ▼
┌──────────────────────────────────────┐
│  1. CANONICALIZATION (Pre-Retrieval) │  ← agent/nodes/retrieve.py
│     - extract_schema_hints()         │
│     - Appends "Schema hints: X≈Y"    │
└────────────────┬─────────────────────┘
                 │
                 ▼
┌──────────────────────────────────────┐
│  2. SCHEMA RETRIEVAL                 │  ← mcp-server/tools/get_semantic_subgraph.py
│     - Vector similarity search       │
│     - Returns tables + columns       │
│     - Enriched with canonical_aliases│
└────────────────┬─────────────────────┘
                 │
                 ▼
┌──────────────────────────────────────┐
│  3. AMBIGUITY RESOLUTION             │  ← mcp-server/services/ambiguity/
│     - MentionExtractor (SpaCy)       │
│     - CandidateBinder (ontology-first)
│     - AmbiguityResolver              │
└────────────────┬─────────────────────┘
                 │
        ┌────────┴────────┐
        ▼                 ▼
     CLEAR             AMBIGUOUS/MISSING
   (continue)          (clarify node)
```

## Key Invariants

1. **Never pass raw user query to retrieval.** Always run `extract_schema_hints()` first.

2. **Binder must check `ent_id` first.** If `mention.metadata["ent_id"]` exists and matches
   a schema element, short-circuit with `ontology_match=1.0`.

3. **Do not mix embedding dimensions.** Schema uses OpenAI 1536-dim. Semantic scoring with
   FastEmbed 384-dim was removed due to incompatibility.

## Telemetry Signals

### retrieve_context_node spans
- `grounding.canonicalization_applied`: bool
- `grounding.schema_hints_count`: int
- `grounding.grounded_query`: string (if applied)

### AmbiguityResolver output
- `grounding_metadata.ent_id_present`: bool
- `grounding_metadata.ontology_match_used`: bool
- `grounding_metadata.schema_candidates_count`: int

## Debugging

If clarification is triggered unexpectedly:

1. Check `grounding.canonicalization_applied == true` → SpaCy enabled?
2. Check `grounding.schema_hints_count > 0` → Patterns matched?
3. Check `grounding_metadata.ontology_match_used == true` → Binder short-circuited?
4. Check `grounding_metadata.schema_candidates_count > 0` → Retrieval returned results?
