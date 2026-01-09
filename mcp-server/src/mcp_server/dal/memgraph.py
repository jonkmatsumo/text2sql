"""Memgraph/Neo4j adapter for GraphStore.

This module implements the GraphStore protocol using the official Neo4j driver.
It strictly converts all driver-specific results (Nodes, Relationships, Paths)
into the canonical DAL types (Node, Edge, GraphData), preventing leakage
of backend-specific objects.
"""

import logging
from typing import Any, Dict, List, Optional

# GraphStore is used for typing compliance check if needed, but if not used in runtime code
# we might only need it for validation. Let's keep it if we want to register it
# or use it for type hints.
from mcp_server.dal.interfaces import GraphStore
from mcp_server.dal.types import Edge, GraphData, Node
from neo4j import Driver, GraphDatabase

# from neo4j.graph import Node as Neo4jNode, Relationship as Neo4jRel


logger = logging.getLogger(__name__)


class MemgraphStore(GraphStore):
    """GraphStore implementation for Memgraph/Neo4j."""

    def __init__(self, uri: str, user: str, password: str):
        """Initialize the Memgraph driver.

        Args:
            uri: Bolt URI (e.g., "bolt://localhost:7687")
            user: Database username
            password: Database password
        """
        auth = (user, password) if user and password else None
        self.driver: Driver = GraphDatabase.driver(uri, auth=auth)

    def close(self):
        """Close the driver connection."""
        self.driver.close()

    def upsert_node(
        self,
        label: str,
        node_id: str,
        properties: Dict[str, Any],
    ) -> Node:
        """Create or update a node.

        Uses Cypher MERGE to ensure idempotency.
        """
        query = f"""
        MERGE (n:`{label}` {{id: $node_id}})
        SET n += $props
        RETURN n
        """

        with self.driver.session() as session:
            result = session.run(query, node_id=node_id, props=properties)
            record = result.single()
            if not record:
                raise RuntimeError(f"Failed to upsert node: {node_id}")

            neo4j_node = record["n"]
            # Map to canonical Node
            # neo4j_node.id is the internal int ID, we rely on our 'id' property
            # But wait, if we defined 'id' in MERGE, it should be there.
            # Let's be careful: Neo4j Node object has .id (int) and .element_id (str)
            # but we want the property 'id' we set.

            props = dict(neo4j_node)
            # Ensure our 'id' is preserved.
            if "id" not in props:
                props["id"] = node_id

            return Node(
                id=str(props["id"]),
                label=list(neo4j_node.labels)[0] if neo4j_node.labels else label,
                properties=props,
            )

    def upsert_edge(
        self,
        source_id: str,
        target_id: str,
        edge_type: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Edge:
        """Create or update an edge."""
        props = properties or {}

        # We assume nodes exist or we merge them?
        # Usually edge upsert assumes nodes exist.
        # Let's match nodes to be safe, or MERGE if we want to create them.
        # Strict graph strictness usually implies nodes should exist.
        # But 'GraphStore' semantics in our app usually allowed lazy creation for some things.
        # Let's stick to MATCHing end nodes for safety to ensure we don't create ghost nodes
        # without labels.

        query = f"""
        MATCH (a {{id: $source_id}}), (b {{id: $target_id}})
        MERGE (a)-[r:`{edge_type}`]->(b)
        SET r += $props
        RETURN r, a.id as source_id, b.id as target_id
        """

        with self.driver.session() as session:
            result = session.run(query, source_id=source_id, target_id=target_id, props=props)
            record = result.single()
            if not record:
                raise RuntimeError(
                    f"Failed to upsert edge: {source_id} -> {target_id}. " f"Do the nodes exist?"
                )

            neo4j_rel = record["r"]
            rel_props = dict(neo4j_rel)

            return Edge(
                source_id=str(record["source_id"]),
                target_id=str(record["target_id"]),
                type=neo4j_rel.type,
                properties=rel_props,
            )

    def get_subgraph(
        self,
        root_id: str,
        depth: int = 1,
        labels: Optional[List[str]] = None,
    ) -> GraphData:
        """Retrieve subgraph.

        Executes a traversal and strictly maps results to GraphData.
        """
        # Determine label filter for traversal target
        label_expr = ""
        if labels:
            label_expr = ":" + "|".join(f"`{lbl}`" for lbl in labels)

        # 0..depth hop expansion
        # Using APOC is common but if standard cypher is preferred we can use variable
        # length paths. The prompt didn't specify APOC. Let's use Standard Cypher for
        # broader compatibility if possible.
        # "MATCH (n {id: $root_id})-[r*0..$depth]-(m)
        # RETURN collect(distinct n) + collect(distinct m), collect(distinct r)"
        # Simple BFS expansion:

        # RE-IMPLEMENTING get_subgraph with explicit return map
        query = f"""
        MATCH p = (root {{id: $root_id}})-[*0..{depth}]-(m{label_expr})
        WITH collect(p) as paths
        CALL {{
            WITH paths
            UNWIND paths as p
            UNWIND nodes(p) as n
            RETURN collect(distinct n) as nodes
        }}
        CALL {{
            WITH paths
            UNWIND paths as p
            UNWIND relationships(p) as r
            RETURN collect(distinct r) as rels
        }}
        RETURN nodes, rels
        """

        with self.driver.session() as session:
            result = session.run(query, root_id=root_id)
            record = result.single()

            final_nodes = []
            if record:
                neo_nodes = record["nodes"]
                neo_rels = record["rels"]

                # Helpers
                def to_canonical_node(n):
                    props = dict(n)
                    # Fallback if 'id' prop is missing (shouldn't happen in our schema)
                    nid = str(props.get("id", n.element_id))
                    return Node(
                        id=nid,
                        label=list(n.labels)[0] if n.labels else "Unknown",
                        properties=props,
                    )

                # Map neo4j nodes to canonical (capture ID map for edges)
                node_map = {}  # element_id -> canonical ID

                for n in neo_nodes:
                    c_node = to_canonical_node(n)
                    final_nodes.append(c_node)
                    node_map[n.element_id] = c_node.id

                final_edges = []
                for r in neo_rels:
                    start_id = node_map.get(r.start_node.element_id)
                    end_id = node_map.get(r.end_node.element_id)

                    if start_id and end_id:
                        final_edges.append(
                            Edge(
                                source_id=start_id,
                                target_id=end_id,
                                type=r.type,
                                properties=dict(r),
                            )
                        )
                    else:
                        logger.warning(
                            f"Skipping edge {r.type} due to missing end nodes in current "
                            f"subgraph view."
                        )

                return GraphData(nodes=final_nodes, edges=final_edges)

            return GraphData()

    def get_nodes(self, label: str) -> List[Node]:
        """Retrieve all nodes with a specific label."""
        query = f"MATCH (n:`{label}`) RETURN n"

        with self.driver.session() as session:
            result = session.run(query)
            nodes = []
            for record in result:
                neo4j_node = record["n"]
                props = dict(neo4j_node)
                # Ensure 'id' is present
                if "id" not in props and hasattr(neo4j_node, "element_id"):
                    # Fallback if property is missing, though our upsert guarantees it.
                    # We prefer the property 'id'.
                    props["id"] = neo4j_node.element_id

                # If props still has no id (unlikely with our upsert), generic fallback?
                # For strictness we might filter or error, but let's be robust.
                if "id" in props:
                    nodes.append(Node(id=str(props["id"]), label=label, properties=props))
            return nodes

    def delete_subgraph(self, root_id: str) -> List[str]:
        """Delete subgraph and return deleted IDs."""
        query = """
        MATCH (root {id: $root_id})-[*0..]->(m)
        WITH m
        DETACH DELETE m
        RETURN m.id as deleted_id
        """
        # Note: [*0..] is indefinite depth - dangerous?
        # Usually we want specific semantics like "Delete Tree".
        # The prompt said: "Return list of deleted node IDs".
        # Assuming safe usage or manageable size.

        with self.driver.session() as session:
            result = session.run(query, root_id=root_id)
            return [record["deleted_id"] for record in result if record["deleted_id"]]
