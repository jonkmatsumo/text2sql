import asyncio

import asyncpg

from common.config.dataset import get_default_db_name
from common.config.env import get_env_int, get_env_str


async def main():
    """Run migration to add ROLLED_BACK status."""
    db_host = get_env_str("DB_HOST", "localhost")
    db_port = get_env_int("DB_PORT", 5432)
    db_name = get_env_str("DB_NAME", get_default_db_name())
    db_user = get_env_str("DB_USER", "text2sql_ro")
    db_pass = get_env_str("DB_PASS", "secure_agent_pass")

    dsn = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

    print(f"Connecting to {dsn}...")

    try:
        conn = await asyncpg.connect(dsn)

        row = await conn.fetchrow(
            """
            SELECT conname
            FROM pg_constraint
            WHERE conrelid = 'nlp_pattern_runs'::regclass
            AND contype = 'c'
            AND pg_get_constraintdef(oid) LIKE '%status%';
        """
        )

        if row:
            conname = row["conname"]
            print(f"Found constraint: {conname}")
            await conn.execute(f"ALTER TABLE nlp_pattern_runs DROP CONSTRAINT {conname}")
            print("Dropped old constraint.")

        await conn.execute(
            """
            ALTER TABLE nlp_pattern_runs
            ADD CONSTRAINT nlp_pattern_runs_status_check
            CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'AWAITING_REVIEW', 'ROLLED_BACK'))
        """
        )
        print("Added new constraint with ROLLED_BACK.")

        await conn.close()

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
