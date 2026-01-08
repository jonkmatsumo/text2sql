"""Semantic subgraph retrieval tool for MCP server.

Uses vector similarity search with tables-first strategy and adaptive thresholding.
"""

import asyncio
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor

from mcp_server.graph_ingestion.indexing import VectorIndexer

logger = logging.getLogger(__name__)

# Search parameters
TABLES_K = 5  # Number of tables to retrieve
COLUMNS_K = 3  # Number of columns to retrieve (fallback only)


def _get_mini_graph(query_text: str) -> dict:
    """Retrieve subgraph synchronously using tables-first strategy.

    Strategy:
    1. Search for k=5 Table nodes with adaptive thresholding
    2. Only search Columns if no tables found (fallback)
    3. Traverse 1 hop from seeds to get related nodes

    Returns:
        Dict with 'nodes' and 'relationships' keys
    """
    uri = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")
    user = os.getenv("MEMGRAPH_USER", "")
    password = os.getenv("MEMGRAPH_PASSWORD", "")

    indexer = VectorIndexer(uri=uri, user=user, password=password)

    try:
        # Tables-first strategy: search tables only
        table_hits = indexer.search_nodes(query_text, label="Table", k=TABLES_K)

        # Fallback: if no tables found, try columns
        if not table_hits:
            logger.info("No table hits, falling back to column search")
            column_hits = indexer.search_nodes(query_text, label="Column", k=COLUMNS_K)
            seeds = column_hits
        else:
            seeds = table_hits
            # Add type annotation for formatter
            for h in seeds:
                h["type"] = "Table"

        if not seeds:
            return {"nodes": [], "relationships": []}

        # Log what we found
        seed_names = [h["node"].get("name") for h in seeds]
        logger.info(
            f"Semantic search found {len(seeds)} seeds: {seed_names[:5]}... "
            f"(best_score={seeds[0]['score']:.3f})"
        )

        # Extract seed table names for traversal
        seed_table_names = []
        for h in seeds:
            node = h["node"]
            if h.get("type") == "Table" or "table" not in node:
                # It's a table node
                seed_table_names.append(node.get("name"))
            else:
                # It's a column node - get parent table
                seed_table_names.append(node.get("table"))

        seed_table_names = [n for n in seed_table_names if n]
        if not seed_table_names:
            return {"nodes": [], "relationships": []}

        # 1-hop traversal from seed tables
        with indexer.driver.session() as session:
            # Simpler traversal query: get tables, their columns, and FK-related tables
            traversal_query = """
            MATCH (t:Table)
            WHERE t.name IN $seed_tables
            OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
            OPTIONAL MATCH (c)-[:FOREIGN_KEY_TO]->(fc:Column)<-[:HAS_COLUMN]-(ft:Table)
            RETURN t, collect(DISTINCT c) as columns,
                   collect(DISTINCT {fk_col: c, ref_col: fc, ref_table: ft}) as fk_info
            """

            result = session.run(traversal_query, seed_tables=seed_table_names)

            nodes_map = {}
            rels_list = []

            def add_node(node, node_type):
                if node is None:
                    return None
                nid = node.element_id if hasattr(node, "element_id") else str(node.id)
                if nid not in nodes_map:
                    props = dict(node)
                    props["id"] = nid
                    props["type"] = node_type
                    # Remove embedding from output
                    props.pop("embedding", None)
                    nodes_map[nid] = props
                return nid

            for record in result:
                # Add table node
                table_node = record["t"]
                table_id = add_node(table_node, "Table")

                # Add table score if available
                table_name = table_node.get("name")
                for h in seeds:
                    if h["node"].get("name") == table_name:
                        nodes_map[table_id]["score"] = h["score"]
                        break

                # Add column nodes and relationships
                for col in record["columns"]:
                    if col is not None:
                        col_id = add_node(col, "Column")
                        if table_id and col_id:
                            rels_list.append(
                                {
                                    "source": table_id,
                                    "target": col_id,
                                    "type": "HAS_COLUMN",
                                }
                            )

                # Add FK relationships and referenced tables
                for fk_info in record["fk_info"]:
                    fk_col = fk_info.get("fk_col")
                    ref_col = fk_info.get("ref_col")
                    ref_table = fk_info.get("ref_table")

                    if fk_col and ref_col and ref_table:
                        fk_col_id = add_node(fk_col, "Column")
                        ref_col_id = add_node(ref_col, "Column")
                        ref_table_id = add_node(ref_table, "Table")

                        if fk_col_id and ref_col_id:
                            rels_list.append(
                                {
                                    "source": fk_col_id,
                                    "target": ref_col_id,
                                    "type": "FOREIGN_KEY_TO",
                                }
                            )
                        if ref_table_id and ref_col_id:
                            rels_list.append(
                                {
                                    "source": ref_table_id,
                                    "target": ref_col_id,
                                    "type": "HAS_COLUMN",
                                }
                            )

            # Dedupe relationships
            seen_rels = set()
            unique_rels = []
            for rel in rels_list:
                key = (rel["source"], rel["target"], rel["type"])
                if key not in seen_rels:
                    seen_rels.add(key)
                    unique_rels.append(rel)

            return {"nodes": list(nodes_map.values()), "relationships": unique_rels}

    except Exception as e:
        logger.error(f"Error in get_semantic_subgraph: {e}")
        return {"error": str(e)}
    finally:
        indexer.close()


async def get_semantic_subgraph(query: str) -> str:
    """Retrieve relevant subgraph of tables and columns based on a natural language query.

    Uses tables-first strategy with adaptive thresholding.

    Args:
        query: The natural language query to search for.

    Returns:
        JSON string containing nodes and relationships of the subgraph.
    """
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        result = await loop.run_in_executor(pool, _get_mini_graph, query)

    return json.dumps(result, separators=(",", ":"))
