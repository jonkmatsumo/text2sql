"""MCP tool: get_semantic_subgraph - Retrieve relevant schema subgraph.

Uses deterministic "Mini-Schema" expansion:
1. Seed selection (adaptive vector search)
2. Seed Table Expansion (get table + columns)
3. Join Discovery (get foreign keys + referenced tables/columns only)
"""

import asyncio
import logging
import time
from typing import Optional

from common.telemetry import Telemetry
from dal.database import Database
from dal.memgraph import MemgraphStore
from ingestion.vector_indexer import VectorIndexer
from mcp_server.services.rag import RagEngine

TOOL_NAME = "get_semantic_subgraph"
TOOL_DESCRIPTION = (
    "Retrieve relevant subgraph of tables and columns based on a natural language query."
)

logger = logging.getLogger(__name__)

# Search parameters
TABLES_K = 5  # Number of tables to retrieve
COLUMNS_K = 3  # Number of columns to retrieve (fallback only)
COLUMN_FALLBACK_MIN_SCORE = 0.6
COLUMN_FALLBACK_GENERIC_STRICT_SCORE = 0.75
COLUMN_FALLBACK_SCORE_SEPARATION = 0.1
GENERIC_COLUMN_NAMES = {"id", "name", "status", "amount"}


def _apply_column_guardrails(seeds: list[dict]) -> tuple[list[dict], bool]:
    """Filter ambiguous column fallback seeds.

    Returns:
        Tuple of (filtered_seeds, relaxed_flag)
    """
    if not seeds:
        return [], False

    relaxed = False
    filtered = [s for s in seeds if s.get("score", 0.0) >= COLUMN_FALLBACK_MIN_SCORE]
    if not filtered:
        relaxed = True
        return seeds[:COLUMNS_K], relaxed

    top_hits = filtered[:COLUMNS_K]
    generic_count = 0
    for hit in top_hits:
        col_name = (hit.get("node", {}).get("name") or "").lower()
        if col_name in GENERIC_COLUMN_NAMES:
            generic_count += 1

    if generic_count >= max(1, len(top_hits) // 2 + 1):
        top_score = top_hits[0].get("score", 0.0)
        second_score = top_hits[1].get("score", 0.0) if len(top_hits) > 1 else 0.0
        strong_separation = (top_score - second_score) >= COLUMN_FALLBACK_SCORE_SEPARATION
        guarded = []
        for hit in filtered:
            col_name = (hit.get("node", {}).get("name") or "").lower()
            is_primary = bool(hit.get("node", {}).get("is_primary_key"))
            if col_name in GENERIC_COLUMN_NAMES and not is_primary and not strong_separation:
                if hit.get("score", 0.0) < COLUMN_FALLBACK_GENERIC_STRICT_SCORE:
                    continue
            guarded.append(hit)

        if not guarded:
            relaxed = True
            return filtered[:COLUMNS_K], relaxed
        return guarded, relaxed

    return filtered, relaxed


async def _get_mini_graph(query_text: str, store: MemgraphStore) -> dict:
    """Retrieve subgraph using deterministic mini-schema expansion.

    Args:
        query_text: User query.
        store: MemgraphStore instance.

    Returns:
        Dict with 'nodes' and 'relationships' keys
    """
    indexer = VectorIndexer(store=store)
    introspector = Database.get_schema_introspector()

    try:
        seed_start = time.monotonic()
        with Telemetry.start_span(
            "seed_selection",
            attributes={
                "seed_selection.k_tables": TABLES_K,
                "seed_selection.k_columns": COLUMNS_K,
            },
        ) as seed_span:
            # 1. Seed Selection (Tables-First)
            table_hits, table_meta = await indexer.search_nodes_with_metadata(
                query_text, label="Table", k=TABLES_K
            )

            column_meta = {"threshold": 0.0, "timing_ms": {}}
            if not table_hits:
                logger.info("No table hits, falling back to column search")
                seeds, column_meta = await indexer.search_nodes_with_metadata(
                    query_text, label="Column", k=COLUMNS_K, use_column_cache=True
                )
                seeds, relaxed_guardrails = _apply_column_guardrails(seeds)
                seed_table_names = list(
                    set(s["node"].get("table") for s in seeds if s["node"].get("table"))
                )
                seed_scores = {s["node"].get("table"): s["score"] for s in seeds}
                seed_span.set_attribute("seed_selection.path", "column_fallback")
                seed_span.set_attribute("seed_selection.column_hit_count", len(seeds))
                seed_span.set_attribute(
                    "seed_selection.column_guardrail_relaxed", relaxed_guardrails
                )
            else:
                seeds = table_hits
                seed_table_names = [s["node"].get("name") for s in seeds]
                seed_scores = {s["node"].get("name"): s["score"] for s in seeds}
                seed_span.set_attribute("seed_selection.path", "table")
                seed_span.set_attribute("seed_selection.column_hit_count", 0)
                seed_span.set_attribute("seed_selection.column_cache_hit", False)
                seed_span.set_attribute("seed_selection.column_guardrail_relaxed", False)

            seed_span.set_attribute("seed_selection.table_hit_count", len(table_hits))
            seed_span.set_attribute(
                "seed_selection.similarity_threshold_table", table_meta.get("threshold", 0.0)
            )
            seed_span.set_attribute(
                "seed_selection.similarity_threshold_column", column_meta.get("threshold", 0.0)
            )
            seed_span.set_attribute(
                "seed_selection.column_cache_hit", column_meta.get("cache_hit", False)
            )

            table_timing = table_meta.get("timing_ms", {})
            column_timing = column_meta.get("timing_ms", {})
            if table_timing:
                seed_span.set_attribute(
                    "seed_selection.latency_ms.table_embedding", table_timing.get("embedding", 0.0)
                )
                seed_span.set_attribute(
                    "seed_selection.latency_ms.table_search", table_timing.get("search", 0.0)
                )
            if column_timing:
                seed_span.set_attribute(
                    "seed_selection.latency_ms.column_embedding",
                    column_timing.get("embedding", 0.0),
                )
                seed_span.set_attribute(
                    "seed_selection.latency_ms.column_search", column_timing.get("search", 0.0)
                )

            seed_span.set_attribute(
                "seed_selection.latency_ms.traversal_start",
                (time.monotonic() - seed_start) * 1000,
            )

        if not seed_table_names:
            return {"nodes": [], "relationships": []}

        logger.info(f"Found seeds: {seed_table_names[:5]}")

        nodes_map = {}
        rels_list = []

        def add_node(node_id, label, properties, node_type, score=None, canonical_aliases=None):
            if node_id not in nodes_map:
                props = dict(properties)
                props["id"] = node_id
                props["name"] = properties.get("name", node_id)
                props["type"] = node_type
                if score is not None:
                    props["score"] = score
                # Add canonical_aliases if provided (Phase B enrichment)
                if canonical_aliases:
                    props["canonical_aliases"] = canonical_aliases
                nodes_map[node_id] = props
            return node_id

        # Step 1: Fetch Seed Tables and their Columns via Introspector
        fk_table_names = set()

        # Pre-load canonical aliases for enrichment
        from mcp_server.services.canonicalization.alias_service import CanonicalAliasService

        try:
            await CanonicalAliasService.load_aliases()
        except Exception as e:
            logger.debug(f"Alias enrichment not available: {e}")

        # We can fetch table definitions in parallel
        tasks = [introspector.get_table_def(t_name) for t_name in seed_table_names]
        table_defs = await asyncio.gather(*tasks, return_exceptions=True)

        for t_name, table_def in zip(seed_table_names, table_defs):
            if isinstance(table_def, Exception):
                logger.warning(f"Failed to fetch table def for {t_name}: {table_def}")
                continue

            # Get canonical aliases for this table (optional enrichment)
            try:
                table_aliases = await CanonicalAliasService.get_aliases_for_table(t_name)
            except Exception:
                table_aliases = []

            # Add Table Node
            t_id = add_node(
                node_id=t_name,
                label="Table",
                properties={"name": t_name, "description": table_def.description},
                node_type="Table",
                score=seed_scores.get(t_name),
                canonical_aliases=table_aliases,
            )

            # Add Columns and Relationships
            for col in table_def.columns:
                c_id = f"{t_name}.{col.name}"
                add_node(
                    node_id=c_id,
                    label="Column",
                    properties={
                        "name": col.name,
                        "table": t_name,
                        "type": col.data_type,
                        "nullable": col.is_nullable,
                    },
                    node_type="Column",
                )
                rels_list.append({"source": t_id, "target": c_id, "type": "HAS_COLUMN"})

            # Discover Foreign Keys
            for fk in table_def.foreign_keys:
                source_c_id = f"{t_name}.{fk.column_name}"
                target_c_id = f"{fk.foreign_table_name}.{fk.foreign_column_name}"

                # We add the target table to our expansion list if not already in seeds
                if fk.foreign_table_name not in seed_table_names:
                    fk_table_names.add(fk.foreign_table_name)

                # Add relationship (target column/table nodes will be added in expansion or
                # if they are seeds)
                rels_list.append(
                    {"source": source_c_id, "target": target_c_id, "type": "FOREIGN_KEY_TO"}
                )

        # Step 2: Expand Foreign Key (Dimension) Tables
        if fk_table_names:
            logger.info(f"Expanding columns for dimension tables: {list(fk_table_names)}")
            fk_tasks = [introspector.get_table_def(t_name) for t_name in fk_table_names]
            fk_table_defs = await asyncio.gather(*fk_tasks, return_exceptions=True)

            for t_name, table_def in zip(fk_table_names, fk_table_defs):
                if isinstance(table_def, Exception):
                    continue

                t_id = add_node(
                    node_id=t_name,
                    label="Table",
                    properties={"name": t_name, "description": table_def.description},
                    node_type="Table",
                )

                for col in table_def.columns:
                    c_id = f"{t_name}.{col.name}"
                    add_node(
                        node_id=c_id,
                        label="Column",
                        properties={
                            "name": col.name,
                            "table": t_name,
                            "type": col.data_type,
                            "nullable": col.is_nullable,
                        },
                        node_type="Column",
                    )
                    rels_list.append({"source": t_id, "target": c_id, "type": "HAS_COLUMN"})

        from mcp_server.services.rag.linker import SchemaLinker

        # Dense Schema Linking (Triple-Filter Pruning)
        tables = []
        col_id_to_node = {}
        for nid, props in nodes_map.items():
            if props.get("type") == "Table":
                props["columns"] = []
                tables.append(props)
            elif props.get("type") == "Column":
                col_id_to_node[nid] = props

        for rel in rels_list:
            if rel["type"] == "HAS_COLUMN":
                t_id = rel["source"]
                c_id = rel["target"]
                if t_id in nodes_map and c_id in col_id_to_node:
                    nodes_map[t_id]["columns"].append(col_id_to_node[c_id])

        await SchemaLinker.rank_and_filter_columns(query_text, tables, target_cols_per_table=15)

        kept_col_ids = set()
        for t in tables:
            for c in t["columns"]:
                kept_col_ids.add(c["id"])
            t.pop("columns", None)

        final_nodes = []
        for nid, props in nodes_map.items():
            if props.get("type") == "Table":
                final_nodes.append(props)
            elif props.get("type") == "Column":
                if nid in kept_col_ids:
                    final_nodes.append(props)
            else:
                final_nodes.append(props)

        final_node_ids = set(n["id"] for n in final_nodes)

        kept_rels = []
        for rel in rels_list:
            if rel["source"] in final_node_ids and rel["target"] in final_node_ids:
                kept_rels.append(rel)

        rels_list = kept_rels

        # Process relationships uniqueness
        seen_rels = set()
        unique_rels = []
        for rel in rels_list:
            key = (rel["source"], rel["target"], rel["type"])
            if key not in seen_rels:
                seen_rels.add(key)
                unique_rels.append(rel)

        return {"nodes": final_nodes, "relationships": unique_rels}

    except Exception as e:
        logger.exception(f"Error in get_semantic_subgraph: {e}")
        return {
            "error": {
                "code": "SEMANTIC_SUBGRAPH_QUERY_FAILED",
                "message": "Failed to retrieve semantic subgraph.",
            }
        }


async def handler(query: str, tenant_id: int = None, snapshot_id: Optional[str] = None) -> str:
    """Retrieve relevant subgraph of tables and columns based on a natural language query.

    Authorization:
        Requires 'SQL_USER_ROLE' (or higher) and valid 'tenant_id'.

    Data Access:
        Read-only access to the graph store (Memgraph) and RAG vector store.
        Results are enriched with schema metadata from the target database.

    Failure Modes:
        - Unauthorized: If tenant_id is missing or role is insufficient.
        - Dependency Failure: If Memgraph or Vector Store is unavailable.
        - Validation Error: If the query is empty or malformed.

    Args:
        query: The natural language query to search for.
        tenant_id: Optional tenant ID for semantic caching.
        snapshot_id: Optional schema snapshot identifier to verify consistency.

    Returns:
        JSON string containing nodes and relationships of the subgraph.
    """
    from mcp_server.utils.errors import build_error_metadata
    from mcp_server.utils.validation import (
        DEFAULT_MAX_INPUT_BYTES,
        require_tenant_id,
        validate_string_length,
    )

    if err := require_tenant_id(tenant_id, TOOL_NAME):
        return err

    if err := validate_string_length(
        query,
        max_bytes=DEFAULT_MAX_INPUT_BYTES,
        param_name="query",
        tool_name=TOOL_NAME,
    ):
        return err

    # Cache Read
    # Cache Read
    embedding = None
    import time

    start_time = time.monotonic()

    # Cache Read
    try:
        cache = Database.get_cache_store()
        embedding = await RagEngine.embed_text(query)

        cached = await cache.lookup(embedding, tenant_id, threshold=0.90, cache_type="subgraph")
        if cached:
            logger.info(f"✓ Cache Hit for semantic subgraph: {query[:50]}...")
            return cached.value
    except Exception as e:
        logger.warning(f"Cache lookup failed: {e}")

    # The Work
    try:
        try:
            store = Database.get_graph_store()
        except Exception as e:
            logger.error(f"Failed to get graph store: {e}")
            from common.models.tool_envelopes import ToolResponseEnvelope

            return ToolResponseEnvelope(
                result={},
                error=build_error_metadata(
                    message="Graph store not authorized or initialized.",
                    category="dependency_failure",
                    provider="graph_store",
                    retryable=False,
                    code="GRAPH_STORE_UNAVAILABLE",
                ),
            ).model_dump_json(exclude_none=True)

        result = await _get_mini_graph(query, store)

        if isinstance(result, dict) and "error" in result:
            from common.models.tool_envelopes import ToolResponseEnvelope

            error_code = "SEMANTIC_SUBGRAPH_QUERY_FAILED"
            error_message = "Semantic subgraph retrieval failed."
            if isinstance(result["error"], dict):
                code_val = result["error"].get("code")
                msg_val = result["error"].get("message")
                if isinstance(code_val, str) and code_val.strip():
                    error_code = code_val
                if isinstance(msg_val, str) and msg_val.strip():
                    error_message = msg_val

            return ToolResponseEnvelope(
                result={},
                error=build_error_metadata(
                    message=error_message,
                    category="invalid_request",
                    provider=Database.get_query_target_provider(),
                    retryable=False,
                    code=error_code,
                ),
            ).model_dump_json(exclude_none=False)

        execution_time_ms = (time.monotonic() - start_time) * 1000

        from common.models.tool_envelopes import GenericToolMetadata, ToolResponseEnvelope

        envelope = ToolResponseEnvelope(
            result=result,
            metadata=GenericToolMetadata(
                provider=Database.get_query_target_provider(),
                execution_time_ms=execution_time_ms,
                snapshot_id=snapshot_id,
            ),
        )
        json_result = envelope.model_dump_json(exclude_none=True)

        # Cache Write
        if tenant_id and embedding and not result.get("error"):
            try:
                cache = Database.get_cache_store()
                await cache.store(
                    user_query=query,
                    generated_sql=json_result,
                    query_embedding=embedding,
                    tenant_id=tenant_id,
                    cache_type="subgraph",
                )
                logger.info(f"✓ Cached semantic subgraph for tenant {tenant_id}")
            except Exception as e:
                logger.warning(f"Cache store failed: {e}")

        return json_result
    except Exception as e:
        _ = e  # keep local exception for logging/debugging only
        logger.exception("Semantic subgraph retrieval failed")
        from common.models.tool_envelopes import ToolResponseEnvelope

        return ToolResponseEnvelope(
            result={},
            error=build_error_metadata(
                message="Semantic subgraph retrieval failed.",
                category="invalid_request",
                provider=Database.get_query_target_provider(),
                retryable=False,
                code="SEMANTIC_SUBGRAPH_FAILED",
            ),
        ).model_dump_json(exclude_none=False)
