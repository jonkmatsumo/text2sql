import logging
from typing import Any, Dict, List

from mcp_server.dal.memgraph import MemgraphStore
from mcp_server.factory.retriever import get_retriever

logger = logging.getLogger(__name__)


class SyncEngine:
    """Synchronizes live PostgreSQL schema with Memgraph using DAL."""

    def __init__(self, graph_uri: str = "bolt://localhost:7687", graph_auth: tuple = None):
        """
        Initialize the Sync Engine.

        Args:
            graph_uri: Memgraph/Neo4j Bolt URI.
            graph_auth: Tuple of (user, password) for Memgraph.
        """
        self.retriever = get_retriever()
        user = graph_auth[0] if graph_auth else ""
        password = graph_auth[1] if graph_auth else ""
        self.store = MemgraphStore(graph_uri, user, password)

    def close(self):
        """Close connections."""
        self.store.close()

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
                    "type": col.data_type,
                    "nullable": True,  # Retriever doesn't expose nullable yet, defaulting
                    "primary_key": col.is_primary_key,
                }
            schema_info["tables"][table_name] = col_dict

        return schema_info

    def get_graph_state(self) -> Dict[str, Any]:
        """Fetch current Tables and Columns from Memgraph via DAL."""
        graph_state = {"tables": {}}

        # 1. Fetch all Tables
        tables = self.store.get_nodes("Table")
        for t in tables:
            # We used 'name' as stored property (and ID)
            t_name = t.properties.get("name")
            if t_name:
                graph_state["tables"][t_name] = {}

        # 2. Fetch all Columns
        # Column nodes have 'table' property and 'name' property
        columns = self.store.get_nodes("Column")
        for c in columns:
            t_name = c.properties.get("table")
            c_name = c.properties.get("name")
            c_type = c.properties.get("type")

            if t_name and c_name and t_name in graph_state["tables"]:
                graph_state["tables"][t_name][c_name] = {"type": c_type}

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

            # Update types
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
        """Delete tables and their connected nodes (columns)."""
        for t_name in table_names:
            # Table ID is just the table name in our Hydrator logic
            self.store.delete_subgraph(t_name)

    def _prune_columns(self, table_name: str, column_names: List[str]):
        """Delete specific columns from a table."""
        for c_name in column_names:
            # Column ID is "TableName.ColumnName"
            col_id = f"{table_name}.{c_name}"
            self.store.delete_subgraph(col_id)

    def _update_column_type(self, table_name: str, col_name: str, new_type: str):
        """Update column type property."""
        col_id = f"{table_name}.{col_name}"
        # Upsert merges properties.
        # Note: We need to pass label (Column) and properties.
        # We don't change structural edges here, just properties.
        self.store.upsert_node(label="Column", node_id=col_id, properties={"type": new_type})
