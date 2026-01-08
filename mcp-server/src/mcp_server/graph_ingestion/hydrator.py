import logging
from typing import Any, Dict

from neo4j import GraphDatabase

logger = logging.getLogger(__name__)


class GraphHydrator:
    """Hydrates Memgraph/Neo4j with schema information."""

    def __init__(self, uri: str = "bolt://localhost:7687", user: str = "", password: str = ""):
        """Initialize the Graph Hydrator."""
        auth = (user, password) if user and password else None
        self.driver = GraphDatabase.driver(uri, auth=auth)

    def close(self):
        """Close the driver connection."""
        self.driver.close()

    def hydrate_schema(self, parsed_schema: Dict[str, Any]):
        """
        Hydrate the graph with tables, columns, and relationships.

        Args:
            parsed_schema: output from SchemaParser.parse()
        """
        logger.info("Starting graph hydration...")
        with self.driver.session() as session:
            # 1. Create Tables and Properties
            for table in parsed_schema.get("tables", []):
                session.execute_write(self._create_table_node, table)

            # 2. Create Columns and HAS_COLUMN relationships
            for table in parsed_schema.get("tables", []):
                session.execute_write(self._create_column_nodes, table)

            # 3. Create Foreign Key relationships
            for table in parsed_schema.get("tables", []):
                session.execute_write(self._create_fk_relationships, table)

        logger.info("Graph hydration complete.")

    def _create_table_node(self, tx, table: Dict[str, Any]):
        """Create or update a Table node."""
        query = """
        MERGE (t:Table {name: $name})
        SET t.schema = $schema,
            t.description = $comment
        """
        tx.run(
            query,
            name=table["table_name"],
            schema=table["schema"],
            comment=table.get("comment") or "",
        )

    def _create_column_nodes(self, tx, table: Dict[str, Any]):
        """Create Column nodes and connect to Table."""
        table_name = table["table_name"]

        for col in table.get("columns", []):
            query = """
            MATCH (t:Table {name: $table_name})
            MERGE (c:Column {name: $col_name, table: $table_name})
            SET c.type = $type,
                c.is_primary_key = $pk,
                c.is_not_null = $nn,
                c.description = $comment
            MERGE (t)-[:HAS_COLUMN]->(c)
            """
            tx.run(
                query,
                table_name=table_name,
                col_name=col["name"],
                type=col["type"],
                pk=col["primary_key"],
                nn=col["not_null"],
                comment=col.get("comment") or "",
            )

    def _create_fk_relationships(self, tx, table: Dict[str, Any]):
        """Create FOREIGN_KEY_TO relationships between columns."""
        source_table_name = table["table_name"]

        for constraint in table.get("constraints", []):
            fk_details = constraint.get("foreign_key")
            if fk_details:
                target_table_name = fk_details["reference_table"]

                source_cols = fk_details["source_columns"]
                target_cols = fk_details["reference_columns"]

                # Assuming simple single-column FKs for now mostly, but handling list
                # logic to mapping index-based if composite
                for idx, src_col in enumerate(source_cols):
                    if idx < len(target_cols):
                        tgt_col = target_cols[idx]

                        query = """
                        MATCH (sc:Column {name: $src_col, table: $src_table})
                        MATCH (tc:Column {name: $tgt_col, table: $tgt_table})
                        MERGE (sc)-[:FOREIGN_KEY_TO]->(tc)
                        """
                        tx.run(
                            query,
                            src_col=src_col,
                            src_table=source_table_name,
                            tgt_col=tgt_col,
                            tgt_table=target_table_name,
                        )
