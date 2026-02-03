import asyncio
from typing import List

from common.interfaces.schema_introspector import SchemaIntrospector
from dal.bigquery.config import BigQueryConfig
from dal.catalog import CatalogNamespace
from schema import ColumnDef, TableDef


class BigQuerySchemaIntrospector(SchemaIntrospector):
    """BigQuery implementation of SchemaIntrospector using INFORMATION_SCHEMA."""

    async def list_table_names(self, schema: str = "public") -> List[str]:
        """List all table names in the configured BigQuery dataset."""
        config = _get_config()
        namespace = CatalogNamespace(config.project, config.dataset)
        query = (
            f"SELECT table_name FROM `{namespace.to_bigquery()}.INFORMATION_SCHEMA.TABLES` "
            "WHERE table_type = 'BASE TABLE' ORDER BY table_name"
        )
        rows = await _run_query(query, config.location)
        return [row["table_name"] for row in rows]

    async def get_table_def(self, table_name: str, schema: str = "public") -> TableDef:
        """Get the full definition of a table (columns)."""
        config = _get_config()
        namespace = CatalogNamespace(config.project, config.dataset)
        query = (
            "SELECT column_name, data_type, is_nullable "
            f"FROM `{namespace.to_bigquery()}.INFORMATION_SCHEMA.COLUMNS` "
            "WHERE table_name = @table_name "
            "ORDER BY ordinal_position"
        )
        rows = await _run_query(
            query,
            config.location,
            parameters={"table_name": table_name},
        )
        columns = [
            ColumnDef(
                name=row["column_name"],
                data_type=row["data_type"],
                is_nullable=(row["is_nullable"] == "YES"),
            )
            for row in rows
        ]
        return TableDef(name=table_name, columns=columns, foreign_keys=[], description=None)

    async def get_sample_rows(
        self, table_name: str, limit: int = 3, schema: str = "public"
    ) -> List[dict]:
        """Fetch sample rows for a BigQuery table."""
        config = _get_config()
        namespace = CatalogNamespace(config.project, config.dataset, table_name)
        query = f"SELECT * FROM `{namespace.to_bigquery()}` LIMIT {int(limit)}"
        rows = await _run_query(query, config.location)
        return rows


def _get_config() -> BigQueryConfig:
    from dal.bigquery.query_target import BigQueryQueryTargetDatabase

    return BigQueryQueryTargetDatabase._config or BigQueryConfig.from_env()


async def _run_query(sql: str, location: str | None, parameters: dict | None = None) -> List[dict]:
    from google.cloud import bigquery

    def _execute():
        client = bigquery.Client()
        job_config = bigquery.QueryJobConfig()
        if parameters:
            job_config.query_parameters = [
                bigquery.ScalarQueryParameter(name, "STRING", value)
                for name, value in parameters.items()
            ]
        job = client.query(sql, job_config=job_config, location=location)
        return [dict(row) for row in job.result()]

    return await asyncio.to_thread(_execute)
