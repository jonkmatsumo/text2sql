"""MCP tool: get_semantic_subgraph - Retrieve relevant schema subgraph.

Uses deterministic "Mini-Schema" expansion:
1. Seed selection (adaptive vector search)
2. Seed Table Expansion (get table + columns)
3. Join Discovery (get foreign keys + referenced tables/columns only)
"""

import asyncio
import json
import logging

from mcp_server.services.rag import RagEngine

from dal.database import Database
from dal.memgraph import MemgraphStore
from ingestion.vector_indexer import VectorIndexer

TOOL_NAME = "get_semantic_subgraph"

logger = logging.getLogger(__name__)

# Search parameters
TABLES_K = 5  # Number of tables to retrieve
COLUMNS_K = 3  # Number of columns to retrieve (fallback only)


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
        # 1. Seed Selection (Tables-First)
        table_hits = await indexer.search_nodes(query_text, label="Table", k=TABLES_K)

        if not table_hits:
            logger.info("No table hits, falling back to column search")
            seeds = await indexer.search_nodes(query_text, label="Column", k=COLUMNS_K)
            seed_table_names = list(
                set(s["node"].get("table") for s in seeds if s["node"].get("table"))
            )
            seed_scores = {s["node"].get("table"): s["score"] for s in seeds}
        else:
            seeds = table_hits
            seed_table_names = [s["node"].get("name") for s in seeds]
            seed_scores = {s["node"].get("name"): s["score"] for s in seeds}

        if not seed_table_names:
            return {"nodes": [], "relationships": []}

        logger.info(f"Found seeds: {seed_table_names[:5]}")

        nodes_map = {}
        rels_list = []

        def add_node(node_id, label, properties, node_type, score=None):
            if node_id not in nodes_map:
                props = dict(properties)
                props["id"] = node_id
                props["name"] = properties.get("name", node_id)
                props["type"] = node_type
                if score is not None:
                    props["score"] = score
                nodes_map[node_id] = props
            return node_id

        # Step 1: Fetch Seed Tables and their Columns via Introspector
        fk_table_names = set()

        # We can fetch table definitions in parallel
        tasks = [introspector.get_table_def(t_name) for t_name in seed_table_names]
        table_defs = await asyncio.gather(*tasks, return_exceptions=True)

        for t_name, table_def in zip(seed_table_names, table_defs):
            if isinstance(table_def, Exception):
                logger.warning(f"Failed to fetch table def for {t_name}: {table_def}")
                continue

            # Add Table Node
            t_id = add_node(
                node_id=t_name,
                label="Table",
                properties={"name": t_name, "description": table_def.description},
                node_type="Table",
                score=seed_scores.get(t_name),
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
        return {"error": str(e)}


async def handler(query: str, tenant_id: int = None) -> str:
    """Retrieve relevant subgraph of tables and columns based on a natural language query.

    Args:
        query: The natural language query to search for.
        tenant_id: Optional tenant ID for semantic caching.

    Returns:
        JSON string containing nodes and relationships of the subgraph.
    """
    # Cache Read
    embedding = None
    if tenant_id:
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
        store = Database.get_graph_store()
    except Exception as e:
        logger.error(f"Failed to get graph store: {e}")
        return json.dumps({"error": "Graph store not authorized or initialized."})

    result = await _get_mini_graph(query, store)

    json_result = json.dumps(result, separators=(",", ":"))

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
