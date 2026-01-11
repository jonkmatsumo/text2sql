import asyncio
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()


async def run_sql_file(conn, file_path):
    """Execute the SQL statements contained in the specified file."""
    print(f"Running {file_path}...")
    with open(file_path, "r") as f:
        sql = f.read()
    try:
        await conn.execute(sql)
        print(f"Successfully ran {file_path}")
    except Exception as e:
        print(f"Error running {file_path}: {e}")


async def main():
    """Apply Phase 5 database schema updates to the project databases."""
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = int(os.getenv("DB_PORT", "5432"))
    db_name = os.getenv("DB_NAME", "pagila")
    db_user = "postgres"
    db_pass = "root_password"

    dsn = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    conn = await asyncpg.connect(dsn)

    # Files to run
    files = [
        "database/control-plane/12-feedback.sql",
        "database/control-plane/13-feedback_submission.sql",
    ]

    for f in files:
        await run_sql_file(conn, f)

        try:
            await conn.execute(
                "ALTER TABLE query_interactions ADD COLUMN tenant_id INTEGER DEFAULT 1;"
            )
        except Exception as e:
            print(f"Skipping tenant_id column: {e}")

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

    await conn.close()
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
