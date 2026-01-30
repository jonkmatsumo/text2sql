import asyncio

import asyncpg

from common.config.dataset import get_default_db_name
from common.config.env import get_env_int, get_env_str


async def main():
    """Run migration to update run items FK."""
    db_host = get_env_str("DB_HOST", "localhost")
    db_port = get_env_int("DB_PORT", 5432)
    db_name = get_env_str("DB_NAME", get_default_db_name())
    db_user = get_env_str("DB_USER", "text2sql_ro")
    db_pass = get_env_str("DB_PASS", "secure_agent_pass")

    dsn = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"

    print(f"Connecting to {dsn}...")

    try:
        conn = await asyncpg.connect(dsn)

        # Update FK constraint to CASCADE
        await conn.execute(
            """
            ALTER TABLE nlp_pattern_run_items
            DROP CONSTRAINT IF EXISTS nlp_pattern_run_items_pattern_label_pattern_text_fkey
        """
        )

        await conn.execute(
            """
            ALTER TABLE nlp_pattern_run_items
            ADD CONSTRAINT nlp_pattern_run_items_pattern_label_pattern_text_fkey
            FOREIGN KEY (pattern_label, pattern_text)
            REFERENCES nlp_patterns(label, pattern)
            ON DELETE CASCADE
        """
        )

        print("Updated nlp_pattern_run_items FK to ON DELETE CASCADE.")

        await conn.close()

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
