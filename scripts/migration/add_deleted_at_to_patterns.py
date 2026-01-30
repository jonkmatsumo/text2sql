import asyncio

import asyncpg

from common.config.dataset import get_default_db_name
from common.config.env import get_env_int, get_env_str


async def main():
    """Run migration to add deleted_at column."""
    db_host = get_env_str("DB_HOST", "localhost")
    db_port = get_env_int("DB_PORT", 5432)
    db_name = get_env_str("DB_NAME", get_default_db_name())
    db_user = get_env_str("DB_USER", "text2sql_ro")
    db_pass = get_env_str("DB_PASS", "secure_agent_pass")

    dsn = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

    print(f"Connecting to {dsn}...")

    try:
        conn = await asyncpg.connect(dsn)

        # Add deleted_at column
        await conn.execute("ALTER TABLE nlp_patterns ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMP")

        print("Added deleted_at column to nlp_patterns.")

        await conn.close()

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
