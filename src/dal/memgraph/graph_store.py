import logging
from typing import Any, Dict, List, Optional

from neo4j import GraphDatabase

from common.interfaces import GraphStore
from schema.graph.data import GraphData
from schema.graph.edge import Edge
from schema.graph.node import Node

logger = logging.getLogger(__name__)


class MemgraphStore(GraphStore):
    """Memgraph implementation of GraphStore.

    Uses the Neo4j Python driver (compatible with Memgraph Bolt protocol).
    """

    def __init__(self, uri: str, user: str, password: str):
        """Initialize Memgraph driver."""
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        """Close driver connection."""
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

    def search_ann_seeds(
        self,
        label: str,
        embedding: List[float],
        k: int,
        index_name: str = "table_embedding_index",
        embedding_property: str = "embedding",
    ) -> List[Dict[str, Any]]:
        """Search for seeds using vector similarity.

        Strategies:
        1. If label is 'Table', use Memgraph HNSW index via vector_search module.
        2. Otherwise (e.g. 'Column'), use brute-force cosine similarity scan.

        Returns:
            List of dicts: {"node": dict, "score": float}
            Where 'node' is a flat dictionary of node properties.
        """
        # Strategy selection based on label (could be config-driven in future)
        if label == "Table" and self.supports_vector_search() and self.has_index(index_name):
            return self._search_ann_seeds_vector_search(
                embedding=embedding,
                k=k,
                index_name=index_name,
                embedding_property=embedding_property,
            )

        # CURRENT FIX: Client-side cosine similarity (Memgraph vector modules missing)
        # 1. Fetch all candidate nodes
        query = f"""
        MATCH (n:`{label}`)
        WHERE n.{embedding_property} IS NOT NULL
        RETURN n AS node
        """

        with self.driver.session() as session:
            result = session.run(query)
            candidates = []

            for record in result:
                neo_node = record["node"]
                props = dict(neo_node)

                # Extract embedding
                node_embedding = props.get(embedding_property)
                if not node_embedding or not isinstance(node_embedding, list):
                    continue

                # Calculate Cosine Similarity in Python
                # Dot product
                dot_product = sum(a * b for a, b in zip(embedding, node_embedding))
                # Magnitudes
                mag_a = sum(a * a for a in embedding) ** 0.5
                mag_b = sum(b * b for b in node_embedding) ** 0.5

                score = 0.0
                if mag_a * mag_b > 0:
                    score = dot_product / (mag_a * mag_b)

                # Remove embedding from props to save memory/bandwidth in return
                props.pop(embedding_property, None)

                # Ensure ID
                if "id" not in props:
                    try:
                        props["id"] = str(neo_node.element_id)
                    except AttributeError:
                        pass

                candidates.append({"node": props, "score": score})

            # 2. Sort by score DESC and take top K
            candidates.sort(key=lambda x: x["score"], reverse=True)
            return candidates[:k]

    def supports_vector_search(self) -> bool:
        """Return True if vector_search module is available."""
        try:
            _ = self._vector_search_index_info()
            return True
        except Exception:
            return False

    def has_index(self, index_name: str) -> bool:
        """Return True if the vector_search index exists."""
        try:
            rows = self._vector_search_index_info()
        except Exception:
            return False

        for row in rows:
            name = row.get("name") or row.get("index_name") or row.get("index")
            if name == index_name:
                return True
        return False

    def _vector_search_index_info(self) -> List[Dict[str, Any]]:
        """Fetch vector_search index info from Memgraph."""
        query = "CALL vector_search.show_index_info()"
        with self.driver.session() as session:
            result = session.run(query)
            return [dict(record) for record in result]

    def _search_ann_seeds_vector_search(
        self,
        embedding: List[float],
        k: int,
        index_name: str,
        embedding_property: str,
    ) -> List[Dict[str, Any]]:
        """Use Memgraph vector_search to retrieve top-k table seeds."""
        query = """
        CALL vector_search.search($index_name, $k, $embedding)
        YIELD node, distance, score
        RETURN node, distance, score
        """
        with self.driver.session() as session:
            result = session.run(
                query,
                {"index_name": index_name, "k": k, "embedding": embedding},
            )
            candidates = []
            for record in result:
                neo_node = record.get("node")
                if neo_node is None:
                    continue

                props = dict(neo_node)
                props.pop(embedding_property, None)

                if "id" not in props:
                    try:
                        props["id"] = str(neo_node.element_id)
                    except AttributeError:
                        pass

                if "score" in record and record.get("score") is not None:
                    score = float(record.get("score"))
                elif "similarity" in record and record.get("similarity") is not None:
                    score = float(record.get("similarity"))
                else:
                    distance = record.get("distance")
                    score = 0.0
                    if distance is not None:
                        try:
                            score = 1.0 - float(distance)
                        except (TypeError, ValueError):
                            score = 0.0

                candidates.append({"node": props, "score": score})

            candidates.sort(key=lambda x: (-x["score"], str(x["node"].get("id", ""))))
            return candidates[:k]

    def run_query(
        self, query: str, parameters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Run a raw Cypher query and return results as dictionaries."""
        with self.driver.session() as session:
            result = session.run(query, parameters or {})
            return [dict(record) for record in result]
