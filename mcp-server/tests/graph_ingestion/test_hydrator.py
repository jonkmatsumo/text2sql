from unittest.mock import MagicMock, patch

from mcp_server.dal.ingestion.hydrator import GraphHydrator, should_skip_column_embedding
from mcp_server.models import ColumnDef, ForeignKeyDef, TableDef


@patch("mcp_server.dal.ingestion.hydrator.MemgraphStore")
@patch("mcp_server.dal.ingestion.hydrator.EmbeddingService")
def test_hydrate_schema(mock_embedding_service, mock_memgraph_store_cls):
    """Test hydrate_schema logic with DAL."""
    # Setup mock store instance
    mock_store = mock_memgraph_store_cls.return_value

    mock_embedding_service.return_value.embed_text.return_value = [0.1, 0.2]

    # Mock Retriever
    mock_retriever = MagicMock()
    t1 = TableDef(name="t1", description="desc", sample_data=[{"a": 1}])
    mock_retriever.list_tables.return_value = [t1]

    c1 = ColumnDef(name="c1", data_type="INTEGER", is_primary_key=True, is_nullable=False)
    mock_retriever.get_columns.return_value = [c1]

    fk1 = ForeignKeyDef(column_name="c1", foreign_table_name="t2", foreign_column_name="c2")
    mock_retriever.get_foreign_keys.return_value = [fk1]

    hydrator = GraphHydrator()
    hydrator.hydrate_schema(mock_retriever)

    # Verify Store Intialization
    mock_memgraph_store_cls.assert_called_once()

    # Verify UPSERT calls
    # 1. Table upsert
    assert mock_store.upsert_node.called

    # 2. Column upsert (should be called for c1)
    # 3. Edge upsert (HAS_COLUMN and FOREIGN_KEY_TO)
    assert mock_store.upsert_edge.called

    # Verify retriever calls
    mock_retriever.list_tables.assert_called_once()
    mock_retriever.get_columns.assert_called_with("t1")
    mock_retriever.get_foreign_keys.assert_called_with("t1")


class TestShouldSkipColumnEmbedding:
    """Tests for the should_skip_column_embedding helper."""

    def test_skip_last_update(self):
        """Low-signal timestamp columns should skip embedding."""
        col = ColumnDef(
            name="last_update", data_type="timestamp", is_primary_key=False, is_nullable=True
        )
        assert should_skip_column_embedding(col, is_fk=False) is True

    def test_skip_created_at(self):
        """Audit columns should skip embedding."""
        col = ColumnDef(
            name="created_at", data_type="timestamp", is_primary_key=False, is_nullable=True
        )
        assert should_skip_column_embedding(col, is_fk=False) is True

    def test_skip_generic_id(self):
        """Generic *_id columns (not PK/FK) should skip embedding."""
        col = ColumnDef(
            name="legacy_id", data_type="integer", is_primary_key=False, is_nullable=True
        )
        assert should_skip_column_embedding(col, is_fk=False) is True

    def test_keep_pk_id(self):
        """Primary key ID columns should keep embedding."""
        col = ColumnDef(
            name="customer_id", data_type="integer", is_primary_key=True, is_nullable=False
        )
        assert should_skip_column_embedding(col, is_fk=False) is False

    def test_keep_fk_id(self):
        """Foreign key ID columns should keep embedding."""
        col = ColumnDef(
            name="customer_id", data_type="integer", is_primary_key=False, is_nullable=True
        )
        assert should_skip_column_embedding(col, is_fk=True) is False

    def test_keep_regular_column(self):
        """Regular columns should keep embedding."""
        col = ColumnDef(
            name="first_name", data_type="varchar", is_primary_key=False, is_nullable=True
        )
        assert should_skip_column_embedding(col, is_fk=False) is False
