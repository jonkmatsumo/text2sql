import json
import logging
import re
from typing import Set, Tuple

from mcp_server.dal.ingestion.indexing import EmbeddingService
from mcp_server.dal.memgraph import MemgraphStore
from mcp_server.dal.retrievers.data_schema_retriever import DataSchemaRetriever
from mcp_server.models import ColumnDef, TableDef

logger = logging.getLogger(__name__)

# Columns to skip embedding generation (still create nodes for joins)
LOW_SIGNAL_COLUMNS = frozenset(
    {
        "last_update",
        "created_at",
        "updated_at",
        "activebool",
        "is_deleted",
        "deleted_at",
        "modified_at",
        "active",
    }
)

# Pattern for generic ID columns (e.g., customer_id, but NOT primary key or foreign key)
ID_COLUMN_PATTERN = re.compile(r".*_id$", re.IGNORECASE)


def should_skip_column_embedding(
    col: ColumnDef,
    is_fk: bool = False,
) -> bool:
    """
    Determine if a column should skip embedding generation.

    We skip embeddings for:
    - Generic timestamp/audit columns (last_update, created_at, etc.)
    - Generic *_id columns that are neither primary keys nor foreign keys

    We still create the Column node - we just don't embed it.
    This prevents vector search matching generic columns across many tables.

    Args:
        col: Column metadata
        is_fk: Whether this column is a foreign key

    Returns:
        True if embedding should be skipped
    """
    name_lower = col.name.lower()

    # Skip known low-signal columns
    if name_lower in LOW_SIGNAL_COLUMNS:
        return True

    # Skip *_id columns unless they are PKs or FKs
    if ID_COLUMN_PATTERN.match(name_lower):
        if col.is_primary_key or is_fk:
            return False  # Keep embeddings for PKs and FKs
        return True  # Skip generic ID columns

    return False


class GraphHydrator:
    """Hydrates Memgraph/Neo4j with schema information using DAL."""

    def __init__(self, uri: str = "bolt://localhost:7687", user: str = "", password: str = ""):
        """Initialize the Graph Hydrator."""
        self.store = MemgraphStore(uri, user, password)
        self.embedding_service = EmbeddingService()

    def close(self):
        """Close the connection."""
        self.store.close()

    def hydrate_schema(self, retriever: DataSchemaRetriever):
        """
        Hydrate the graph with tables, columns, and relationships.

        Args:
            retriever: DataSchemaRetriever instance to fetch schema.
        """
        logger.info("Starting graph hydration...")

        # Fetch all tables first
        tables = retriever.list_tables()
        logger.info(f"Found {len(tables)} tables to hydrate.")

        # Build FK lookup for all tables (to know which columns are FKs)
        fk_columns: Set[Tuple[str, str]] = set()
        all_fks = {}
        for table in tables:
            try:
                fks = retriever.get_foreign_keys(table.name)
                all_fks[table.name] = fks
                for fk in fks:
                    fk_columns.add((table.name, fk.column_name))
            except Exception as e:
                logger.error(f"Error fetching FKs for table {table.name}: {e}")
                all_fks[table.name] = []

        skipped_embeddings = 0

        # Process each table completely (Table Node + Columns)
        for table in tables:
            try:
                # Fetch columns first
                columns = retriever.get_columns(table.name)

                # 1. Create Table Node (Enriched with column names)
                self._create_table_node(table, columns)

                # 2. Create Column Nodes
                skipped = self._create_column_nodes(table.name, columns, fk_columns)
                skipped_embeddings += skipped

            except Exception as e:
                logger.error(f"Error hydrating table {table.name}: {e}")

        if skipped_embeddings > 0:
            logger.info(f"Skipped embeddings for {skipped_embeddings} low-signal columns")

        # 3. Create Foreign Key relationships (done after all nodes exist)
        for table in tables:
            fks = all_fks.get(table.name, [])
            if fks:
                self._create_fk_relationships(table.name, fks)

        logger.info("Graph hydration complete.")

    def _create_table_node(self, table: TableDef, columns: list[ColumnDef]):
        """Create or update a Table node."""
        # Generate embedding for the table
        # We embed name, description, AND column names for better semantic search
        # "PG movies" -> matches column "rating" or "description"
        col_names = ", ".join([c.name for c in columns])
        embedding_text = (
            f"Table: {table.name}\n"
            f"Columns: {col_names}\n"
            f"Description: {table.description or ''}"
        )
        embedding = self.embedding_service.embed_text(embedding_text)

        # Serialize sample data (handle datetime objects)
        sample_data_json = json.dumps(table.sample_data, default=str) if table.sample_data else "[]"

        self.store.upsert_node(
            label="Table",
            node_id=table.name,  # Using name as ID for Tables
            properties={
                "name": table.name,
                "description": table.description or "",
                "sample_data": sample_data_json,
                "embedding": embedding,
            },
        )

    def _create_column_nodes(
        self, table_name: str, columns: list[ColumnDef], fk_columns: set
    ) -> int:
        """Create Column nodes and connect to Table.

        Returns the count of columns where embedding was skipped.
        """
        skipped_count = 0

        for col in columns:
            is_fk = (table_name, col.name) in fk_columns
            skip_embedding = should_skip_column_embedding(col, is_fk)

            if skip_embedding:
                skipped_count += 1
                embedding = None  # No embedding for low-signal columns
            else:
                # Generate embedding for the column
                embedding_text = (
                    f"Column: {col.name}\n"
                    f"Table: {table_name}\n"
                    f"Type: {col.data_type}\n"
                    f"Description: {col.description or ''}"
                )
                embedding = self.embedding_service.embed_text(embedding_text)

            # Node ID: "TableName.ColumnName" to be unique
            col_node_id = f"{table_name}.{col.name}"

            # 1. Upsert Column Node
            self.store.upsert_node(
                label="Column",
                node_id=col_node_id,
                properties={
                    "name": col.name,
                    "table": table_name,
                    "type": col.data_type,
                    "is_primary_key": col.is_primary_key,
                    "description": col.description or "",
                    "embedding": embedding,
                },
            )

            # 2. Upsert HAS_COLUMN edge (Table -> Column)
            self.store.upsert_edge(
                source_id=table_name,  # Table ID
                target_id=col_node_id,  # Column ID
                edge_type="HAS_COLUMN",
            )

        return skipped_count

    def _create_fk_relationships(self, table_name: str, fks: list):
        """Create FOREIGN_KEY_TO relationships between columns."""
        for fk in fks:
            src_col_id = f"{table_name}.{fk.column_name}"
            tgt_col_id = f"{fk.foreign_table_name}.{fk.foreign_column_name}"

            self.store.upsert_edge(
                source_id=src_col_id, target_id=tgt_col_id, edge_type="FOREIGN_KEY_TO"
            )
