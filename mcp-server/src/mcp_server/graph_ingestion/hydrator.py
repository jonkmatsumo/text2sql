import json
import logging
import re

from mcp_server.graph_ingestion.indexing import EmbeddingService
from mcp_server.models.schema import ColumnMetadata, TableMetadata
from mcp_server.retrievers.data_schema_retriever import DataSchemaRetriever
from neo4j import GraphDatabase

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
    col: ColumnMetadata,
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
    """Hydrates Memgraph/Neo4j with schema information."""

    def __init__(self, uri: str = "bolt://localhost:7687", user: str = "", password: str = ""):
        """Initialize the Graph Hydrator."""
        auth = (user, password) if user and password else None
        self.driver = GraphDatabase.driver(uri, auth=auth)
        self.embedding_service = EmbeddingService()

    def close(self):
        """Close the driver connection."""
        self.driver.close()

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
        fk_columns = set()
        all_fks = {}
        for table in tables:
            try:
                fks = retriever.get_foreign_keys(table.name)
                all_fks[table.name] = fks
                for fk in fks:
                    fk_columns.add((table.name, fk.source_col))
            except Exception as e:
                logger.error(f"Error fetching FKs for table {table.name}: {e}")
                all_fks[table.name] = []

        with self.driver.session() as session:
            # 1. Create Tables and Properties
            for table in tables:
                session.execute_write(self._create_table_node, table)

            # 2. Create Columns and HAS_COLUMN relationships
            skipped_embeddings = 0
            for table in tables:
                try:
                    columns = retriever.get_columns(table.name)
                    skipped = session.execute_write(
                        self._create_column_nodes, table.name, columns, fk_columns
                    )
                    skipped_embeddings += skipped
                except Exception as e:
                    logger.error(f"Error fetching columns for table {table.name}: {e}")

            if skipped_embeddings > 0:
                logger.info(f"Skipped embeddings for {skipped_embeddings} low-signal columns")

            # 3. Create Foreign Key relationships
            for table in tables:
                fks = all_fks.get(table.name, [])
                if fks:
                    session.execute_write(self._create_fk_relationships, table.name, fks)

        logger.info("Graph hydration complete.")

    def _create_table_node(self, tx, table: TableMetadata):
        """Create or update a Table node."""
        # Generate embedding for the table
        # We embed the name and description for semantic search
        embedding_text = f"Table: {table.name}\nDescription: {table.description or ''}"
        embedding = self.embedding_service.embed_text(embedding_text)

        # Serialize sample data (handle datetime objects)
        sample_data_json = json.dumps(table.sample_data, default=str) if table.sample_data else "[]"

        query = """
        MERGE (t:Table {name: $name})
        SET t.description = $description,
            t.sample_data = $sample_data,
            t.embedding = $embedding
        """
        tx.run(
            query,
            name=table.name,
            description=table.description or "",
            sample_data=sample_data_json,
            embedding=embedding,
        )

    def _create_column_nodes(
        self, tx, table_name: str, columns: list[ColumnMetadata], fk_columns: set
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
                    f"Type: {col.type}\n"
                    f"Description: {col.description or ''}"
                )
                embedding = self.embedding_service.embed_text(embedding_text)

            # Create node with or without embedding
            query = """
            MATCH (t:Table {name: $table_name})
            MERGE (c:Column {name: $col_name, table: $table_name})
            SET c.type = $type,
                c.is_primary_key = $pk,
                c.description = $description,
                c.embedding = $embedding
            MERGE (t)-[:HAS_COLUMN]->(c)
            """
            tx.run(
                query,
                table_name=table_name,
                col_name=col.name,
                type=col.type,
                pk=col.is_primary_key,
                description=col.description or "",
                embedding=embedding,
            )

        return skipped_count

    def _create_fk_relationships(self, tx, table_name: str, fks: list):
        """Create FOREIGN_KEY_TO relationships between columns."""
        for fk in fks:
            query = """
            MATCH (sc:Column {name: $src_col, table: $src_table})
            MATCH (tc:Column {name: $tgt_col, table: $tgt_table})
            MERGE (sc)-[:FOREIGN_KEY_TO]->(tc)
            """
            tx.run(
                query,
                src_col=fk.source_col,
                src_table=table_name,
                tgt_col=fk.target_col,
                tgt_table=fk.target_table,
            )
