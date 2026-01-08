from unittest.mock import MagicMock, patch

from mcp_server.graph_ingestion.hydrator import GraphHydrator
from mcp_server.models.schema import ColumnMetadata, ForeignKey, TableMetadata


@patch("mcp_server.graph_ingestion.hydrator.GraphDatabase")
@patch("mcp_server.graph_ingestion.hydrator.EmbeddingService")
def test_hydrate_schema(mock_embedding_service, mock_graph_db):
    """Test hydrate_schema logic."""
    mock_driver = MagicMock()
    mock_graph_db.driver.return_value = mock_driver
    mock_session = MagicMock()
    mock_driver.session.return_value.__enter__.return_value = mock_session

    mock_embedding_service.return_value.embed_text.return_value = [0.1, 0.2]

    # Mock Retriever
    mock_retriever = MagicMock()
    t1 = TableMetadata(name="t1", description="desc", sample_data=[{"a": 1}])
    mock_retriever.list_tables.return_value = [t1]

    c1 = ColumnMetadata(name="c1", type="INTEGER", is_primary_key=True)
    mock_retriever.get_columns.return_value = [c1]

    fk1 = ForeignKey(source_col="c1", target_table="t2", target_col="c2")
    mock_retriever.get_foreign_keys.return_value = [fk1]

    hydrator = GraphHydrator()
    hydrator.hydrate_schema(mock_retriever)

    # Verify driver calls
    mock_graph_db.driver.assert_called_once()
    mock_driver.session.assert_called_once()

    # Verify session execution calls (3 phases)
    assert mock_session.execute_write.call_count == 3

    # Verify retriever calls
    mock_retriever.list_tables.assert_called_once()
    mock_retriever.get_columns.assert_called_with("t1")
    mock_retriever.get_foreign_keys.assert_called_with("t1")
