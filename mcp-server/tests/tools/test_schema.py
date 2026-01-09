import json
from unittest.mock import MagicMock, patch

from mcp_server.models.database.column_def import ColumnDef
from mcp_server.models.database.foreign_key_def import ForeignKeyDef
from mcp_server.models.database.table_def import TableDef
from mcp_server.tools.schema import get_sample_data, get_table_schema, list_tables


@patch("mcp_server.tools.schema.get_retriever")
def test_list_tables(mock_get_retriever):
    """Test list_tables tool."""
    mock_retriever = MagicMock()
    mock_retriever.list_tables.return_value = [
        TableDef(name="t1", description="desc", sample_data=[])
    ]
    mock_get_retriever.return_value = mock_retriever

    result_json = list_tables()
    result = json.loads(result_json)

    assert len(result) == 1
    assert result[0]["name"] == "t1"
    assert result[0]["description"] == "desc"
    mock_retriever.list_tables.assert_called_once()


@patch("mcp_server.tools.schema.get_retriever")
def test_get_table_schema(mock_get_retriever):
    """Test get_table_schema tool."""
    mock_retriever = MagicMock()
    mock_retriever.get_columns.return_value = [
        ColumnDef(name="c1", data_type="int", is_nullable=True, is_primary_key=True)
    ]
    mock_retriever.get_foreign_keys.return_value = [
        ForeignKeyDef(column_name="c1", foreign_table_name="t2", foreign_column_name="c2")
    ]
    mock_get_retriever.return_value = mock_retriever

    # Pass list of tables
    result_json = get_table_schema(["t1"])
    result = json.loads(result_json)

    assert len(result) == 1
    table_schema = result[0]
    assert table_schema["table_name"] == "t1"
    assert len(table_schema["columns"]) == 1
    assert table_schema["columns"][0]["name"] == "c1"
    assert len(table_schema["foreign_keys"]) == 1
    assert table_schema["foreign_keys"][0]["foreign_table_name"] == "t2"

    mock_retriever.get_columns.assert_called_with("t1")
    mock_retriever.get_foreign_keys.assert_called_with("t1")


@patch("mcp_server.tools.schema.get_retriever")
def test_get_sample_data(mock_get_retriever):
    """Test get_sample_data tool."""
    mock_retriever = MagicMock()
    mock_retriever.get_sample_rows.return_value = [{"a": 1}]
    mock_get_retriever.return_value = mock_retriever

    result_json = get_sample_data("t1", limit=5)
    result = json.loads(result_json)

    assert len(result) == 1
    assert result[0] == {"a": 1}
    mock_retriever.get_sample_rows.assert_called_with("t1", 5)
