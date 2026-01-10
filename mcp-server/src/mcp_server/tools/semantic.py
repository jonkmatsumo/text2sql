"""Semantic subgraph retrieval tool for MCP server.

Uses deterministic "Mini-Schema" expansion:
1. Seed selection (adaptive vector search)
2. Seed Table Expansion (get table + columns)
3. Join Discovery (get foreign keys + referenced tables/columns only)
"""

import asyncio
import json
import logging

from mcp_server.config.database import Database
from mcp_server.dal.executor import ContextAwareExecutor
from mcp_server.dal.ingestion.indexing import VectorIndexer
from mcp_server.dal.memgraph import MemgraphStore

logger = logging.getLogger(__name__)

# Search parameters
TABLES_K = 5  # Number of tables to retrieve
COLUMNS_K = 3  # Number of columns to retrieve (fallback only)


def _get_mini_graph(query_text: str, store: MemgraphStore) -> dict:
    """Retrieve subgraph synchronously using deterministic mini-schema expansion.

    Args:
        query_text: User query.
        store: MemgraphStore instance.

    Returns:
        Dict with 'nodes' and 'relationships' keys
    """
    # Reuse existing store connection
    indexer = VectorIndexer(store=store)

    try:
        # 1. Seed Selection (Tables-First)
        table_hits = indexer.search_nodes(query_text, label="Table", k=TABLES_K)

        if not table_hits:
            logger.info("No table hits, falling back to column search")
            seeds = indexer.search_nodes(query_text, label="Column", k=COLUMNS_K)
            # Map column seeds to their parent tables
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

        def add_node(node, node_type, score=None):
            if node is None:
                return None
            nid = node.element_id if hasattr(node, "element_id") else str(node.id)
            if nid not in nodes_map:
                props = dict(node)
                props["id"] = nid
                props["type"] = node_type
                props.pop("embedding", None)
                if score is not None:
                    props["score"] = score
                nodes_map[nid] = props
            return nid

        # Use the store's driver session
        with store.driver.session() as session:
            # Step 1: Fetch Seed Tables and their Columns
            # We fetch all columns but will rely on formatter to truncate if too many.
            query_step1 = """
            MATCH (t:Table)
            WHERE t.name IN $seed_tables
            OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
            RETURN t, collect(c) as columns
            """
            result1 = session.run(query_step1, seed_tables=seed_table_names)

            for record in result1:
                t = record["t"]
                columns = record["columns"]
                t_name = t.get("name")

                # Add Table
                t_id = add_node(t, "Table", score=seed_scores.get(t_name))

                # Add All Columns (capped by formatter later)
                for col in columns:
                    if col:
                        c_id = add_node(col, "Column")
                        if t_id and c_id:
                            rels_list.append({"source": t_id, "target": c_id, "type": "HAS_COLUMN"})

            # Step 2: Join Discovery
            # Trace FKs from seed tables to *referenced* tables.
            # Initially only include the referenced *columns* (PK).
            query_step2 = """
            MATCH (t:Table) WHERE t.name IN $seed_tables
            MATCH (t)-[:HAS_COLUMN]->(sc:Column)-[:FOREIGN_KEY_TO]->(tc:Column)
            MATCH (rt:Table)-[:HAS_COLUMN]->(tc)
            RETURN t.name as source_table_name,
                   sc as source_col,
                   rt as target_table,
                   tc as target_col
            """
            result2 = session.run(query_step2, seed_tables=seed_table_names)

            # Track newly discovered FK tables (dimension tables)
            fk_table_names = set()

            for record in result2:
                # source_table_name = record["source_table_name"] # Already processed in Step 1
                source_col = record["source_col"]  # Already processed in Step 1
                target_table = record["target_table"]
                target_col = record["target_col"]

                sc_id = add_node(source_col, "Column")

                # Add Referenced Table (rt)
                rt_id = add_node(target_table, "Table")
                rt_name = target_table.get("name") if target_table else None
                if rt_name and rt_name not in seed_table_names:
                    fk_table_names.add(rt_name)

                # Add Referenced Column (tc) - this is the "Bridge" logic
                tc_id = add_node(target_col, "Column")

                # Add relationships
                if sc_id and tc_id:
                    rels_list.append({"source": sc_id, "target": tc_id, "type": "FOREIGN_KEY_TO"})

                if rt_id and tc_id:
                    rels_list.append({"source": rt_id, "target": tc_id, "type": "HAS_COLUMN"})

            # Step 2.5: Full Column Expansion for Dimension Tables
            # Fetch ALL columns for FK-referenced tables (not just the PK).
            # This ensures dimension table columns like `language.name` are available.
            if fk_table_names:
                logger.info(f"Expanding columns for dimension tables: {list(fk_table_names)}")
                query_step2_5 = """
                MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
                WHERE t.name IN $fk_tables
                RETURN t, c
                """
                result2_5 = session.run(query_step2_5, fk_tables=list(fk_table_names))

                for record in result2_5:
                    t = record["t"]
                    c = record["c"]
                    t_id = add_node(t, "Table")
                    c_id = add_node(c, "Column")
                    if t_id and c_id:
                        rels_list.append({"source": t_id, "target": c_id, "type": "HAS_COLUMN"})

        from mcp_server.services.schema_linker import SchemaLinker

        # --- DENSE SCHEMA LINKING (Triple-Filter Pruning) ---
        # 1. Regroup Nodes (Attach columns to tables)
        # nodes_map values are dicts (properties). We can attach 'columns' list to them temporarily.
        tables = []
        col_id_to_node = {}
        for nid, props in nodes_map.items():
            if props.get("type") == "Table":
                props["columns"] = []  # Initialize
                tables.append(props)
            elif props.get("type") == "Column":
                col_id_to_node[nid] = props

        # Populate 'columns' list in table props using HAS_COLUMN edges
        for rel in rels_list:
            if rel["type"] == "HAS_COLUMN":
                t_id = rel["source"]
                c_id = rel["target"]
                if t_id in nodes_map and c_id in col_id_to_node:
                    nodes_map[t_id]["columns"].append(col_id_to_node[c_id])

        # 2. Run Schema Linker
        # Modifies 'columns' list in-place to keep only relevant ones
        SchemaLinker.rank_and_filter_columns(query_text, tables, target_cols_per_table=15)

        # 3. Prune Graph (Rebuild nodes and edges)
        kept_col_ids = set()
        for t in tables:
            for c in t["columns"]:
                kept_col_ids.add(c["id"])
            # Remove the temp 'columns' key to keep response clean
            t.pop("columns", None)

        # Filter Nodes
        final_nodes = []
        for nid, props in nodes_map.items():
            if props.get("type") == "Table":
                final_nodes.append(props)
            elif props.get("type") == "Column":
                if nid in kept_col_ids:
                    final_nodes.append(props)
            else:
                final_nodes.append(props)  # Other types if any

        # Filter Relationships
        # Only keep edges where both source/target exist in final_nodes
        final_node_ids = set(n["id"] for n in final_nodes)

        kept_rels = []
        for rel in rels_list:
            if rel["source"] in final_node_ids and rel["target"] in final_node_ids:
                kept_rels.append(rel)

        rels_list = kept_rels
        # --- END DENSE SCHEMA LINKING ---

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
        logger.error(f"Error in get_semantic_subgraph: {e}")
        return {"error": str(e)}


async def get_semantic_subgraph(query: str) -> str:
    """Retrieve relevant subgraph of tables and columns based on a natural language query.

    Args:
        query: The natural language query to search for.

    Returns:
        JSON string containing nodes and relationships of the subgraph.
    """
    try:
        store = Database.get_graph_store()
    except Exception as e:
        logger.error(f"Failed to get graph store: {e}")
        return json.dumps({"error": "Graph store not authorized or initialized."})

    loop = asyncio.get_running_loop()
    # Use ContextAwareExecutor to ensure contextvars (e.g. tenant_id) propagate
    with ContextAwareExecutor() as pool:
        result = await loop.run_in_executor(pool, _get_mini_graph, query, store)

    return json.dumps(result, separators=(",", ":"))
