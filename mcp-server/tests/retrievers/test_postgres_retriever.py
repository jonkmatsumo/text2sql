from unittest.mock import MagicMock, patch

import pytest
from mcp_server.dal.retrievers.postgres_retriever import PostgresRetriever
from mcp_server.models.schema import ColumnMetadata, ForeignKey, TableMetadata


@pytest.fixture
def mock_engine_and_inspector():
    """Mock SQLAlchemy engine and inspector."""
    with patch(
        "mcp_server.dal.retrievers.postgres_retriever.create_engine"
    ) as mock_create_engine, patch(
        "mcp_server.dal.retrievers.postgres_retriever.inspect"
    ) as mock_inspect:

        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        mock_inspector = MagicMock()
        mock_inspect.return_value = mock_inspector

        yield mock_engine, mock_inspector


class TestPostgresRetriever:
    """Test suite for PostgresRetriever."""

    def test_list_tables(self, mock_engine_and_inspector):
        """Test listing tables."""
        mock_engine, mock_inspector = mock_engine_and_inspector

        # Setup mocks
        mock_inspector.get_table_names.return_value = ["table1"]
        mock_inspector.get_table_comment.return_value = {"text": "Table description"}

        # Mock connection and result for sample rows
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn
        mock_result = MagicMock()
        # Mock row mapping behavior
        row = MagicMock()
        row._mapping = {"col1": "val1"}
        mock_result.__iter__.return_value = [row]
        mock_conn.execute.return_value = mock_result

        retriever = PostgresRetriever(
            connection_string="postgresql://test:test@localhost:5432/test"
        )
        tables = retriever.list_tables()

        assert len(tables) == 1
        assert isinstance(tables[0], TableMetadata)
        assert tables[0].name == "table1"
        assert tables[0].description == "Table description"
        assert tables[0].sample_data == [{"col1": "val1"}]

    def test_get_columns(self, mock_engine_and_inspector):
        """Test getting column details."""
        _, mock_inspector = mock_engine_and_inspector

        # Setup mocks
        mock_inspector.get_columns.return_value = [
            {"name": "id", "type": "INTEGER", "comment": "Primary Key"},
            {"name": "name", "type": "VARCHAR", "comment": None},
        ]
        mock_inspector.get_pk_constraint.return_value = {"constrained_columns": ["id"]}

        retriever = PostgresRetriever(connection_string="test")
        columns = retriever.get_columns("table1")

        assert len(columns) == 2

        # Check first column (PK)
        assert isinstance(columns[0], ColumnMetadata)
        assert columns[0].name == "id"
        assert columns[0].type == "INTEGER"
        assert columns[0].is_primary_key is True
        assert columns[0].description == "Primary Key"

        # Check second column
        assert columns[1].name == "name"
        assert columns[1].type == "VARCHAR"
        assert columns[1].is_primary_key is False
        assert columns[1].description is None

    def test_get_foreign_keys(self, mock_engine_and_inspector):
        """Test getting foreign keys."""
        _, mock_inspector = mock_engine_and_inspector

        # Setup mocks
        mock_inspector.get_foreign_keys.return_value = [
            {
                "referred_table": "other_table",
                "constrained_columns": ["fk_col"],
                "referred_columns": ["id"],
            }
        ]

        retriever = PostgresRetriever(connection_string="test")
        fks = retriever.get_foreign_keys("table1")

        assert len(fks) == 1
        assert isinstance(fks[0], ForeignKey)
        assert fks[0].source_col == "fk_col"
        assert fks[0].target_table == "other_table"
        assert fks[0].target_col == "id"

    def test_get_sample_rows(self, mock_engine_and_inspector):
        """Test fetching sample rows."""
        mock_engine, _ = mock_engine_and_inspector

        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        mock_result = MagicMock()
        row1 = MagicMock()
        row1._mapping = {"a": 1}
        row2 = MagicMock()
        row2._mapping = {"a": 2}
        mock_result.__iter__.return_value = [row1, row2]
        mock_conn.execute.return_value = mock_result

        retriever = PostgresRetriever(connection_string="test")
        rows = retriever.get_sample_rows("table1", limit=2)

        assert len(rows) == 2
        assert rows[0] == {"a": 1}
        assert rows[1] == {"a": 2}

        # Verify query execution
        mock_conn.execute.assert_called_once()
        args, kwargs = mock_conn.execute.call_args
        # Check for limit parameter
        assert args[1]["limit"] == 2
