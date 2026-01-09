import logging
import os
import re
from typing import Dict, List

from mcp_server.dal.retrievers.data_schema_retriever import DataSchemaRetriever
from mcp_server.models import ColumnDef, ForeignKeyDef, TableDef
from sqlalchemy import create_engine, inspect, text

logger = logging.getLogger(__name__)

# Pattern for Postgres partition tables (e.g., payment_p2022_01)
PARTITION_TABLE_PATTERN = re.compile(r".*_p\d{4}_\d{2}$")

# Technical/system tables to exclude
EXCLUDED_TABLES = frozenset(
    {
        "schema_migrations",
        "flyway_schema_history",
        "alembic_version",
        "django_migrations",
        "ar_internal_metadata",
        "__diesel_schema_migrations",
    }
)


class PostgresRetriever(DataSchemaRetriever):
    """PostgreSQL implementation of DataSchemaRetriever using SQLAlchemy."""

    def __init__(self, connection_string: str = None):
        """Initialize with connection string (defaults to DATABASE_URL)."""
        if not connection_string:
            connection_string = os.getenv("DATABASE_URL")
            if not connection_string:
                # Fallback to defaults or raise error
                # For local dev, maybe construct from parts
                user = os.getenv("DB_USER", "postgres")
                password = os.getenv("DB_PASS", "postgres")
                host = os.getenv("DB_HOST", "localhost")
                port = os.getenv("DB_PORT", "5432")
                dbname = os.getenv("DB_NAME", "postgres")
                connection_string = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

        self.engine = create_engine(connection_string)
        self.inspector = inspect(self.engine)

    def list_tables(self) -> List[TableDef]:
        """List all tables in the public schema, filtering out partitions and system tables."""
        table_names = self.inspector.get_table_names(schema="public")

        # Filter out partition tables and technical tables
        filtered_names = []
        excluded_count = 0
        for name in table_names:
            if PARTITION_TABLE_PATTERN.match(name):
                excluded_count += 1
                continue
            if name.lower() in EXCLUDED_TABLES:
                excluded_count += 1
                continue
            filtered_names.append(name)

        if excluded_count > 0:
            logger.info(
                f"Excluded {excluded_count} partition/system tables from {len(table_names)} total"
            )

        tables = []
        for name in filtered_names:
            # SQLAlchemy doesn't always easily get table comments depending on dialect version
            # But we can try inspector.get_table_comment(name, schema='public')
            comment_info = self.inspector.get_table_comment(name, schema="public")
            description = comment_info.get("text")

            # Get sample rows
            samples = self.get_sample_rows(name, limit=3)

            tables.append(TableDef(name=name, description=description, sample_data=samples))
        return tables

    def get_columns(self, table_name: str) -> List[ColumnDef]:
        """Get column details using inspector."""
        columns_info = self.inspector.get_columns(table_name, schema="public")
        pk_constraint = self.inspector.get_pk_constraint(table_name, schema="public")
        pk_columns = set(pk_constraint.get("constrained_columns", []))

        columns = []
        for col in columns_info:
            name = col["name"]
            # Type is an object, convert to string
            col_type = str(col["type"])
            description = col.get("comment")
            is_pk = name in pk_columns

            is_nullable = col.get("nullable", True)
            columns.append(
                ColumnDef(
                    name=name,
                    data_type=col_type,
                    is_nullable=is_nullable,
                    is_primary_key=is_pk,
                    description=description,
                )
            )
        return columns

    def get_foreign_keys(self, table_name: str) -> List[ForeignKeyDef]:
        """Get foreign keys using inspector."""
        fks_info = self.inspector.get_foreign_keys(table_name, schema="public")
        fks = []
        for fk in fks_info:
            # PostgreSQL FKs can be composite, but for simplicity here we assume single col
            # or we might need to handle composite keys better in the model.
            # Current model: source_col (str).
            # If multiple, we might create multiple entries or change model.
            # For this iteration, let's just take the first column if multiple.

            target_table = fk["referred_table"]
            constrained_columns = fk["constrained_columns"]
            referred_columns = fk["referred_columns"]

            for src, ref in zip(constrained_columns, referred_columns):
                fks.append(
                    ForeignKeyDef(
                        column_name=src, foreign_table_name=target_table, foreign_column_name=ref
                    )
                )
        return fks

    def get_sample_rows(self, table_name: str, limit: int = 3) -> List[Dict]:
        """Fetch sample rows."""
        with self.engine.connect() as conn:
            # Use text() for safety, though table_name logic should be safe from list_tables
            # Quoting table name is good practice
            query = text(f'SELECT * FROM public."{table_name}" LIMIT :limit')
            result = conn.execute(query, {"limit": limit})
            # Convert rows to dicts
            return [dict(row._mapping) for row in result]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Manual verification
    # Ensure env vars are set or rely on defaults
    # Assuming standard local setup
    try:
        retriever = PostgresRetriever()
        print("Connected to PostgreSQL.")

        tables = retriever.list_tables()
        print(f"Found {len(tables)} tables.")

        if tables:
            first_table = tables[0].name
            print(f"-- Inspecting table: {first_table} --")

            cols = retriever.get_columns(first_table)
            print(f"Columns: {len(cols)}")
            for c in cols:
                print(f" - {c.name} ({c.data_type}) PK={c.is_primary_key}")

            fks = retriever.get_foreign_keys(first_table)
            print(f"Foreign Keys: {len(fks)}")
            for k in fks:
                print(f" - {k.column_name} -> {k.foreign_table_name}.{k.foreign_column_name}")

            print(f"Sample Data: {len(tables[0].sample_data)} rows")

    except Exception as e:
        print(f"Verification failed: {e}")
