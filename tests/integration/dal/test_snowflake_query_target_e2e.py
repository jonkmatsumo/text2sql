import json
import os
import uuid

import pytest
import snowflake.connector

from dal.database import Database
from mcp_server.tools.execute_sql_query import handler as execute_sql_query


def _required_env() -> dict:
    required_keys = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
    ]
    missing = [key for key in required_keys if not os.environ.get(key)]
    if missing:
        return {}
    if not (os.environ.get("SNOWFLAKE_PASSWORD") or os.environ.get("SNOWFLAKE_AUTHENTICATOR")):
        return {}
    return {key: os.environ.get(key) for key in required_keys}


@pytest.mark.asyncio
async def test_snowflake_query_target_introspection_and_exec():
    """Exercise Snowflake introspection + parameterized execution via execute_sql_query."""
    env = _required_env()
    if not env:
        pytest.skip("Snowflake credentials not configured for integration tests.")

    table_name = f"T2SQL_USERS_{uuid.uuid4().hex[:8]}".upper()

    conn = snowflake.connector.connect(
        account=os.environ.get("SNOWFLAKE_ACCOUNT"),
        user=os.environ.get("SNOWFLAKE_USER"),
        password=os.environ.get("SNOWFLAKE_PASSWORD"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
        database=os.environ.get("SNOWFLAKE_DATABASE"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA"),
        role=os.environ.get("SNOWFLAKE_ROLE"),
        authenticator=os.environ.get("SNOWFLAKE_AUTHENTICATOR"),
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(f'CREATE OR REPLACE TABLE "{table_name}" (id INT, name STRING)')
            cursor.execute(f"INSERT INTO \"{table_name}\" (id, name) VALUES (1, 'Ada')")
            cursor.execute(f"INSERT INTO \"{table_name}\" (id, name) VALUES (2, 'Bob')")

        os.environ["QUERY_TARGET_BACKEND"] = "snowflake"
        await Database.init()
        try:
            introspector = Database.get_schema_introspector()
            table_names = await introspector.list_table_names()
            assert table_name in table_names

            result_json = await execute_sql_query(
                f'SELECT "name" FROM "{table_name}" WHERE id = $1',
                tenant_id=1,
                params=[1],
            )
            data = json.loads(result_json)
            assert data["rows"] == [{"name": "Ada"}]
        finally:
            await Database.close()
    finally:
        with conn.cursor() as cursor:
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        conn.close()
