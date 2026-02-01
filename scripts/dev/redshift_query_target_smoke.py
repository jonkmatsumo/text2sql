import asyncio
import json
import os

from dal.database import Database
from mcp_server.tools.execute_sql_query import handler as execute_sql_query


async def main() -> None:
    """Run a simple Redshift query-target smoke test."""
    os.environ.setdefault("QUERY_TARGET_BACKEND", "redshift")
    await Database.init()
    try:
        introspector = Database.get_schema_introspector()
        tables = await introspector.list_table_names()
        print("tables:", tables[:10])

        if tables:
            table = tables[0]
            sample = await introspector.get_sample_rows(table, limit=1)
            print("sample rows:", json.dumps(sample, default=str))

        result = await execute_sql_query("SELECT 1 AS ok", tenant_id=1)
        print("query result:", result)
    finally:
        await Database.close()


if __name__ == "__main__":
    asyncio.run(main())
