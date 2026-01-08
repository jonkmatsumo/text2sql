import asyncio
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor

from mcp_server.graph_ingestion.indexing import VectorIndexer

logger = logging.getLogger(__name__)


def _get_mini_graph(query_text: str, k: int = 3) -> dict:
    """Retrieve subgraph synchronously."""
    # Use environment variables for connection
    uri = os.getenv("MEMGRAPH_URI", "bolt://localhost:7687")
    user = os.getenv("MEMGRAPH_USER", "")
    password = os.getenv("MEMGRAPH_PASSWORD", "")

    # We need to instantiate VectorIndexer here or reuse a global one.
    # Creating per-request for safety in async context, though less efficient.
    # In a real app we'd use a dependency injection or singleton pattern.
    indexer = VectorIndexer(uri=uri, user=user, password=password)

    try:
        # 1. Search for Seed Nodes (Tables and Columns)
        # We search both and combine results
        table_hits = indexer.search_nodes(query_text, label="Table", k=k)
        column_hits = indexer.search_nodes(query_text, label="Column", k=k)

        # Normalize and combine
        # Hits are: [{"node": {...}, "score": float}, ...]
        combined = []
        for h in table_hits:
            h["type"] = "Table"
            combined.append(h)
        for h in column_hits:
            h["type"] = "Column"
            combined.append(h)

        # Sort by score descending and take top k
        combined.sort(key=lambda x: x["score"], reverse=True)
        top_seeds = combined[:k]

        if not top_seeds:
            return {"nodes": [], "relationships": []}

        # 2. Extract Seed IDs
        # Node properties from Memgraph might have 'id' or 'name'.
        # Assuming DataSchemaRetriever/Hydrator used 'id' or 'name' as key.
        # VectorIndexer returns 'node' as dict.
        # Let's verify what keys are allowed. Usually Memgraph internal ID is <id>,
        # but we probably matched on properties. Hydrator creates nodes.
        # Let's hope 'name' is unique enough or we use internal IDs if we can get them.
        # For simplicity, we'll traverse from the matched node themselves if we can pass IDs.
        # Use internal elementId or id if available, or name if unique.
        # VectorIndexer.search_nodes returns dict(node).
        # We need to execute a cypher query starting from these nodes.
        # A robust way is to pass the names if they are unique keys (Table name, Column name).

        seed_names = [h["node"].get("name") for h in top_seeds if h["node"].get("name")]
        if not seed_names:
            return {"nodes": [], "relationships": []}

        # 3. Traversal (2 hops)
        # We want to find tables/columns related to these seeds.
        # (Seed)-[*1..2]-(Target)
        # We'll use the driver directly.

        with indexer.driver.session() as session:
            # Cypher to traverse from seeds
            # Matches any node with name in seed_names, then expands 1-2 hops.
            # Collects nodes and rels.
            traversal_query = """
            MATCH (n)
            WHERE n.name IN $seed_names
            CALL {
                WITH n
                MATCH (n)-[r*1..2]-(m)
                RETURN m, r
            }
            WITH n, collect(m) as related, collect(r) as rels
            RETURN n, related, rels
            """

            # Note: Variable length path return is a list of relationships.
            # We need to be careful with 'r'.
            # Flattening the graph for output.

            result = session.run(traversal_query, seed_names=seed_names)

            nodes_map = {}
            rels_map = {}

            def add_node(node_dict, labels):
                # Simple ID: internal ID or name
                # Memgraph driver node object has.id (int) and .labels (set)
                # But here we might get dicts or Node objects depending on how we return.
                # In 'search_nodes' we did dict(node).
                # session.run returns Record with Node objects.
                nid = (
                    node_dict.element_id if hasattr(node_dict, "element_id") else str(node_dict.id)
                )
                props = dict(node_dict)
                props["id"] = nid
                # Add first label found (Table or Column)
                props["type"] = list(labels)[0] if labels else "Unknown"
                nodes_map[nid] = props

            def add_rel(rel):
                rid = rel.element_id if hasattr(rel, "element_id") else str(rel.id)
                start = (
                    rel.start_node.element_id
                    if hasattr(rel.start_node, "element_id")
                    else str(rel.start_node.id)
                )
                end = (
                    rel.end_node.element_id
                    if hasattr(rel.end_node, "element_id")
                    else str(rel.end_node.id)
                )

                rels_map[rid] = {
                    "id": rid,
                    "source": start,
                    "target": end,
                    "type": rel.type,
                    "properties": dict(rel),
                }

            for record in result:
                # Seed node
                seed_node = record["n"]
                add_node(seed_node, seed_node.labels)

                # Related nodes
                for related_node in record["related"]:
                    add_node(related_node, related_node.labels)

                # Relationships
                # rels is a list of list of relationships (paths) or just list?
                # The query "MATCH (n)-[r*1..2]-(m)" returns 'r' as a LIST of relationships
                # in the path. So 'rels' in RETURN is a LIST of LISTS of relationships.
                for path_rels in record["rels"]:
                    for rel in path_rels:
                        add_rel(rel)

            return {"nodes": list(nodes_map.values()), "relationships": list(rels_map.values())}

    except Exception as e:
        logger.error(f"Error in get_semantic_subgraph: {e}")
        return {"error": str(e)}
    finally:
        indexer.close()


async def get_semantic_subgraph(query: str) -> str:
    """Retrieve relevant subgraph of tables and columns based on a natural language query.

    Use this to understand database structure.

    Args:
        query: The natural language query to search for.

    Returns:
         JSON string containing nodes and relationships of the subgraph.
    """
    # Run synchronous Memgraph operations in a thread pool
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        result = await loop.run_in_executor(pool, _get_mini_graph, query)

    return json.dumps(result, indent=2)
