import asyncio

from mcp_server.config.database import Database


async def migrate():
    """Run database migrations for multi-tenancy and feedback status."""
    await Database.init()
    async with Database.get_connection() as conn:
        print("Adding tenant_id to query_interactions...")
        try:
            await conn.execute(
                "ALTER TABLE query_interactions ADD COLUMN tenant_id INTEGER DEFAULT 1;"
            )
        except Exception as e:
            print(f"Skipping tenant_id column: {e}")

        print("Updating review_queue status constraint...")
        try:
            await conn.execute(
                "ALTER TABLE review_queue DROP CONSTRAINT IF EXISTS review_queue_status_check;"
            )
            await conn.execute(
                "ALTER TABLE review_queue ADD CONSTRAINT review_queue_status_check "
                "CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED', 'NEEDS_FIX', 'PUBLISHED'));"
            )
        except Exception as e:
            print(f"Error updating constraint: {e}")

    print("Done.")


if __name__ == "__main__":
    asyncio.run(migrate())
