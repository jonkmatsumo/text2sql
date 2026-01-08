import json
import logging

from mcp_server.graph_ingestion.indexing import EmbeddingService
from mcp_server.models.schema import ColumnMetadata, TableMetadata
from mcp_server.retrievers.data_schema_retriever import DataSchemaRetriever
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)


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

        with self.driver.session() as session:
            # 1. Create Tables and Properties
            for table in tables:
                session.execute_write(self._create_table_node, table)

            # 2. Create Columns and HAS_COLUMN relationships
            for table in tables:
                try:
                    columns = retriever.get_columns(table.name)
                    session.execute_write(self._create_column_nodes, table.name, columns)
                except Exception as e:
                    logger.error(f"Error fetching columns for table {table.name}: {e}")

            # 3. Create Foreign Key relationships
            for table in tables:
                try:
                    fks = retriever.get_foreign_keys(table.name)
                    if fks:
                        session.execute_write(self._create_fk_relationships, table.name, fks)
                except Exception as e:
                    logger.error(f"Error fetching FKs for table {table.name}: {e}")

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

    def _create_column_nodes(self, tx, table_name: str, columns: list[ColumnMetadata]):
        """Create Column nodes and connect to Table."""
        for col in columns:
            # Generate embedding for the column
            # Embed name, type, and description
            embedding_text = (
                f"Column: {col.name}\n"
                f"Table: {table_name}\n"
                f"Type: {col.type}\n"
                f"Description: {col.description or ''}"
            )
            embedding = self.embedding_service.embed_text(embedding_text)

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
