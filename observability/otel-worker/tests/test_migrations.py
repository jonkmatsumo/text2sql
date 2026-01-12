import os
import unittest

"""Tests for OTEL migrations."""

import sqlalchemy as sa
from alembic import command
from alembic.config import Config
from dotenv import load_dotenv
from otel_worker.config import settings
from sqlalchemy import text

# Load root .env if it exists
dotenv_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))),
    ".env",
)
load_dotenv(dotenv_path)


class TestOtelMigrations(unittest.TestCase):
    """Integrated test suite for OTEL schema migrations."""

    @classmethod
    def setUpClass(cls):
        """Set up engine and alembic config."""
        cls.test_schema = "otel_test_migrations"
        cls.engine = sa.create_engine(settings.POSTGRES_URL)

        # Prepare alembic config
        cls.alembic_cfg = Config("alembic.ini")
        # Ensure we are in the correct directory for alembic to find migrations/
        cls.base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def setUp(self):
        """Reset test schema before each test."""
        # Set env var for env.py
        os.environ["OTEL_DB_SCHEMA"] = self.test_schema
        with self.engine.connect() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {self.test_schema} CASCADE"))
            conn.commit()

    def tearDown(self):
        """Drop test schema after each test."""
        with self.engine.connect() as conn:
            conn.execute(text(f"DROP SCHEMA IF EXISTS {self.test_schema} CASCADE"))
            conn.commit()
        if "OTEL_DB_SCHEMA" in os.environ:
            del os.environ["OTEL_DB_SCHEMA"]

    def test_migration_lifecycle(self):
        """Verify that migrations apply cleanly to an empty schema and can be downgraded."""
        # Run upgrade
        os.chdir(self.base_path)
        command.upgrade(self.alembic_cfg, "head")

        # Verify tables exist
        with self.engine.connect() as conn:
            res = conn.execute(
                text(
                    f"SELECT table_name FROM information_schema.tables "
                    f"WHERE table_schema = '{self.test_schema}'"
                )
            )
            tables = [r[0] for r in res]
            self.assertIn("traces", tables)
            self.assertIn("spans", tables)
            self.assertIn("alembic_version", tables)

        # Run downgrade
        command.downgrade(self.alembic_cfg, "base")

        # Verify tables are gone (downgrade base drops them)
        with self.engine.connect() as conn:
            res = conn.execute(
                text(
                    f"SELECT table_name FROM information_schema.tables "
                    f"WHERE table_schema = '{self.test_schema}'"
                )
            )
            tables = [r[0] for r in res]
            self.assertNotIn("traces", tables)
            self.assertNotIn("spans", tables)

    def test_adoption_and_renaming(self):
        """Verify that existing legacy tables are correctly adopted and renamed."""
        # 1. Manually create legacy schema
        with self.engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.test_schema}"))
            conn.execute(
                text(
                    f"""
                CREATE TABLE {self.test_schema}.traces (
                    trace_id TEXT PRIMARY KEY,
                    start_ts TIMESTAMPTZ,
                    end_ts TIMESTAMPTZ,
                    service_name TEXT
                )
            """
                )
            )
            conn.execute(
                text(
                    f"""
                INSERT INTO {self.test_schema}.traces (trace_id, start_ts, service_name)
                VALUES ('t1', now(), 'legacy-service')
            """
                )
            )
            conn.commit()

        # 2. Run migration
        os.chdir(self.base_path)
        command.upgrade(self.alembic_cfg, "head")

        # 3. Verify renaming and data preservation
        with self.engine.connect() as conn:
            # Check for new column name
            res = conn.execute(
                text(
                    f"SELECT trace_id, start_time, service_name " f"FROM {self.test_schema}.traces"
                )
            )
            row = res.fetchone()
            self.assertEqual(row[0], "t1")
            self.assertEqual(row[2], "legacy-service")

            # Check that new columns were added
            res = conn.execute(text(f"SELECT resource_attributes FROM {self.test_schema}.traces"))
            self.assertIsNone(res.fetchone()[0])

            # Verify spans table was also created
            res = conn.execute(
                text(
                    f"SELECT table_name FROM information_schema.tables "
                    f"WHERE table_schema = '{self.test_schema}' AND table_name = 'spans'"
                )
            )
            self.assertIsNotNone(res.fetchone())


if __name__ == "__main__":
    unittest.main()
