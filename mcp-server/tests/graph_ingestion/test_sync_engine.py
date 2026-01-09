from unittest.mock import MagicMock, patch

from mcp_server.graph_ingestion.sync_engine import SyncEngine
from mcp_server.models.schema import ColumnMetadata, TableMetadata


@patch("mcp_server.graph_ingestion.sync_engine.get_retriever")
@patch("mcp_server.graph_ingestion.sync_engine.MemgraphStore")
def test_sync_engine_init(mock_memgraph_store_cls, mock_get_retriever):
    """Test initialization."""
    SyncEngine()
    mock_get_retriever.assert_called_once()
    mock_memgraph_store_cls.assert_called_once()


@patch("mcp_server.graph_ingestion.sync_engine.get_retriever")
@patch("mcp_server.graph_ingestion.sync_engine.MemgraphStore")
def test_get_live_schema(mock_memgraph_store_cls, mock_get_retriever):
    """Test get_live_schema uses retriever and formats correctly."""
    mock_retriever = MagicMock()

    # Mock tables
    t1 = TableMetadata(name="t1", description="desc")
    mock_retriever.list_tables.return_value = [t1]

    # Mock columns
    c1 = ColumnMetadata(name="c1", type="INTEGER", is_primary_key=True)
    mock_retriever.get_columns.return_value = [c1]

    mock_get_retriever.return_value = mock_retriever

    engine = SyncEngine()
    schema = engine.get_live_schema()

    assert "t1" in schema["tables"]
    assert "c1" in schema["tables"]["t1"]
    col_info = schema["tables"]["t1"]["c1"]
    assert col_info["type"] == "INTEGER"
    assert col_info["primary_key"] is True

    mock_retriever.list_tables.assert_called_once()
    mock_retriever.get_columns.assert_called_with("t1")
