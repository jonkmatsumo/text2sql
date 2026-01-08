import logging
from typing import Any, Dict, List

from mcp_server.factory.retriever import get_retriever
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)


class SyncEngine:
    """Synchronizes live PostgreSQL schema with Memgraph."""

    def __init__(self, graph_uri: str = "bolt://localhost:7687", graph_auth: tuple = None):
        """
        Initialize the Sync Engine.

        Args:
            graph_uri: Memgraph/Neo4j Bolt URI.
            graph_auth: Tuple of (user, password) for Memgraph.
        """
        self.retriever = get_retriever()
        self.graph_driver = GraphDatabase.driver(graph_uri, auth=graph_auth)

    def close(self):
        """Close connections."""
        # Retriever singleton doesn't adhere to close semantics here usually,
        # or we could add close to it, but for now just close graph driver.
        self.graph_driver.close()

    def get_live_schema(self) -> Dict[str, Any]:
        """Fetch current tables and columns from PostgreSQL using Retriever."""
        schema_info = {"tables": {}}

        tables = self.retriever.list_tables()
        for table in tables:
            table_name = table.name
            columns = self.retriever.get_columns(table_name)

            col_dict = {}
            for col in columns:
                col_dict[col.name] = {
                    "type": col.type,
                    "nullable": True,  # Retriever doesn't expose nullable yet, defaulting
                    "primary_key": col.is_primary_key,
                }
            schema_info["tables"][table_name] = col_dict

        return schema_info

    def get_graph_state(self) -> Dict[str, Any]:
        """Fetch current Tables and Columns from Memgraph."""
        graph_state = {"tables": {}}

        with self.graph_driver.session() as session:
            # Fetch all tables and their columns
            query = """
            MATCH (t:Table)
            OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
            RETURN t.name as table, c.name as column, c.type as type
            """
            result = session.run(query)

            for record in result:
                table = record["table"]
                column = record["column"]
                col_type = record["type"]

                if table not in graph_state["tables"]:
                    graph_state["tables"][table] = {}

                if column:  # Might be None if table has no columns (unlikely but possible)
                    graph_state["tables"][table][column] = {"type": col_type}

        return graph_state

    def reconcile_graph(self):
        """Compare live schema with graph and update/prune."""
        logger.info("Starting schema reconciliation...")

        live_schema = self.get_live_schema()
        graph_state = self.get_graph_state()

        live_tables = set(live_schema["tables"].keys())
        graph_tables = set(graph_state["tables"].keys())

        # 1. Prune missing tables
        tables_to_remove = graph_tables - live_tables
        if tables_to_remove:
            logger.info(f"Pruning {len(tables_to_remove)} tables: {tables_to_remove}")
            self._prune_tables(list(tables_to_remove))

        # 2. Check for missing/extra columns in existing tables
        common_tables = live_tables.intersection(graph_tables)
        for table in common_tables:
            live_cols = set(live_schema["tables"][table].keys())
            graph_cols = set(graph_state["tables"][table].keys())

            # Prune columns
            cols_to_remove = graph_cols - live_cols
            if cols_to_remove:
                logger.info(f"Pruning columns from {table}: {cols_to_remove}")
                self._prune_columns(table, list(cols_to_remove))

            # Add/Update columns (simplified: relying on Hydrator or subsequent logic to add)
            # This sync engine primarily focuses on pruning and property updates.
            # Adding new things is often handled by the Hydrator running on the parsed schema.
            # But we can verify type changes here.

            common_cols = live_cols.intersection(graph_cols)
            for col in common_cols:
                live_type = live_schema["tables"][table][col]["type"]
                graph_type = graph_state["tables"][table][col].get("type")

                # Loose comparison as SQL types vs String representation might vary
                if graph_type and live_type != graph_type:
                    logger.info(f"Updating type for {table}.{col}: {graph_type} -> {live_type}")
                    self._update_column_type(table, col, live_type)

        logger.info("Reconciliation complete.")

    def _prune_tables(self, table_names: List[str]):
        """Delete tables and their connected nodes."""
        with self.graph_driver.session() as session:
            query = """
            MATCH (t:Table)
            WHERE t.name IN $names
            DETACH DELETE t
            """
            session.run(query, names=table_names)

            # Also cleanup orphaned columns?
            # Columns are usually connected to tables.
            # If table is deleted, we should delete columns too.
            # The query above only deletes Table nodes.
            # Columns might remain if they are separate nodes.
            # Better approach:
            query_cascade = """
            MATCH (t:Table)
            WHERE t.name IN $names
            OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
            DETACH DELETE t, c
            """
            session.run(query_cascade, names=table_names)

    def _prune_columns(self, table_name: str, column_names: List[str]):
        """Delete specific columns from a table."""
        with self.graph_driver.session() as session:
            query = """
            MATCH (t:Table {name: $table})-[:HAS_COLUMN]->(c:Column)
            WHERE c.name IN $cols
            DETACH DELETE c
            """
            session.run(query, table=table_name, cols=column_names)

    def _update_column_type(self, table_name: str, col_name: str, new_type: str):
        """Update column type property."""
        with self.graph_driver.session() as session:
            query = """
            MATCH (t:Table {name: $table})-[:HAS_COLUMN]->(c:Column {name: $col})
            SET c.type = $new_type
            """
            session.run(query, table=table_name, col=col_name, new_type=new_type)
