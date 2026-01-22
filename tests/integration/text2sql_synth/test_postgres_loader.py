import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# Mock potential missing dependencies for the loader
mock_psycopg = MagicMock()
mock_psycopg.sql = MagicMock()
mock_psycopg2 = MagicMock()
mock_psycopg2.sql = MagicMock()

# Setup sys.modules before importing the loader
with patch.dict("sys.modules", {"psycopg": mock_psycopg, "psycopg2": mock_psycopg2}):
    from text2sql_synth.loaders.postgres import _get_column_type, load_from_manifest


class TestPostgresLoader(unittest.TestCase):
    """Tests for the Postgres manifest loader."""

    def setUp(self):
        """Create a temp manifest with CSV files."""
        self.test_dir = Path(tempfile.mkdtemp())
        self.csv_dir = self.test_dir / "csv"
        self.csv_dir.mkdir()

        self.manifest_data = {
            "tables": {
                "dim_time": {"columns": ["date_key", "full_date"]},
                "dim_institution": {"columns": ["institution_id", "institution_name"]},
            },
            "files": [
                {"table": "dim_time", "file": "csv/dim_time.csv", "format": "csv"},
                {"table": "dim_institution", "file": "csv/dim_institution.csv", "format": "csv"},
            ],
        }
        self.manifest_path = self.test_dir / "manifest.json"
        with open(self.manifest_path, "w") as f:
            json.dump(self.manifest_data, f)

        # Create empty CSVs
        for table in ["dim_time", "dim_institution"]:
            with open(self.csv_dir / f"{table}.csv", "w") as f:
                f.write("col1,col2\nval1,val2")

    def tearDown(self):
        """Remove temp directories and files."""
        shutil.rmtree(self.test_dir)

    def test_column_type_inference(self):
        """Infer column types from names."""
        self.assertEqual(_get_column_type("customer_id"), "TEXT")
        self.assertEqual(_get_column_type("transaction_ts"), "TIMESTAMP")
        self.assertEqual(_get_column_type("gross_amount"), "NUMERIC")
        self.assertEqual(_get_column_type("is_active"), "BOOLEAN")
        self.assertEqual(_get_column_type("year"), "INTEGER")
        self.assertEqual(_get_column_type("unknown_col"), "TEXT")

    def test_load_from_manifest_mocked(self):
        """Load via mocked psycopg3 client."""
        # Reset psycopg mocks
        mock_psycopg.reset_mock()
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_psycopg.connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        # Mock psycopg3 copy
        mock_copy = MagicMock()
        mock_cur.copy.return_value.__enter__.return_value = mock_copy

        with patch.dict("sys.modules", {"psycopg": mock_psycopg}):
            load_from_manifest(self.manifest_path, "postgres://dsn", target_schema="test_schema")

        # Check schema creation
        mock_cur.execute.assert_any_call('CREATE SCHEMA IF NOT EXISTS "test_schema"')

        # Check table creation
        self.assertTrue(
            any(
                'CREATE TABLE IF NOT EXISTS "test_schema"."dim_time"' in str(c)
                for c in mock_cur.execute.call_args_list
            )
        )
        self.assertTrue(
            any(
                'CREATE TABLE IF NOT EXISTS "test_schema"."dim_institution"' in str(c)
                for c in mock_cur.execute.call_args_list
            )
        )

        # Check truncate
        mock_cur.execute.assert_any_call('TRUNCATE TABLE "test_schema"."dim_time" CASCADE')

        # Check COPY calls
        self.assertEqual(mock_cur.copy.call_count, 2)

    def test_load_from_manifest_psycopg2_mocked(self):
        """Load via mocked psycopg2 client."""
        # Reset mocks
        mock_psycopg2.reset_mock()
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        # Psycopg2 cursor doesn't have .copy attribute
        del mock_cur.copy

        mock_psycopg2.connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        with patch.dict("sys.modules", {"psycopg": None, "psycopg2": mock_psycopg2}):
            load_from_manifest(self.manifest_path, "postgres://dsn")

            # Check copy_expert calls
            self.assertTrue(mock_cur.copy_expert.called)
            all_copy_calls = [call[0][0] for call in mock_cur.copy_expert.call_args_list]
            self.assertTrue(any('COPY "public"."dim_time"' in c for c in all_copy_calls))
            self.assertTrue(any('COPY "public"."dim_institution"' in c for c in all_copy_calls))


if __name__ == "__main__":
    unittest.main()
