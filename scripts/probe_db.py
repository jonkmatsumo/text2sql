import asyncio

import asyncpg
from dotenv import load_dotenv

load_dotenv()


async def probe(db_name):
    """Check connection and list public tables for a given database."""
    print(f"--- Probing DB: {db_name} ---")
    dsn = f"postgresql://postgres:root_password@localhost:5432/{db_name}"
    try:
        conn = await asyncpg.connect(dsn)
        rows = await conn.fetch(
            "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public';"
        )
        tables = [r["tablename"] for r in rows]
        print(f"Tables: {tables}")
        await conn.close()
    except Exception as e:
        print(f"Error connecting to {db_name}: {e}")


async def main():
    """Probe the main pagila database and the internal control database."""
    await probe("pagila")
    await probe("text2sql_control")


if __name__ == "__main__":
    asyncio.run(main())
