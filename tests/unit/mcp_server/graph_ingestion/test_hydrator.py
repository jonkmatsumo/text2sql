from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from common.interfaces import GraphStore
from ingestion.graph_hydrator import GraphHydrator, should_skip_column_embedding
from schema import ColumnDef, ForeignKeyDef, TableDef


@patch("ingestion.graph_hydrator.EmbeddingService")
@pytest.mark.asyncio
async def test_hydrate_schema(mock_embedding_service):
    """Test hydrate_schema logic with DAL."""
    # Setup mock store instance
    mock_store = MagicMock(spec=GraphStore)

    mock_embedding_service.return_value.embed_text.return_value = [0.1, 0.2]

    # Mock Introspector
    mock_introspector = MagicMock()
    mock_introspector.list_table_names = AsyncMock(return_value=["t1"])

    t1_def = TableDef(
        name="t1",
        description="desc",
        columns=[ColumnDef(name="c1", data_type="INTEGER", is_primary_key=True, is_nullable=False)],
        foreign_keys=[
            ForeignKeyDef(column_name="c1", foreign_table_name="t2", foreign_column_name="c2")
        ],
    )
    mock_introspector.get_table_def = AsyncMock(return_value=t1_def)
    mock_introspector.get_sample_rows = AsyncMock(return_value=[{"a": 1}])

    hydrator = GraphHydrator(store=mock_store)
    await hydrator.hydrate_schema(mock_introspector)

    # Verify UPSERT calls
    # 1. Table upsert
    assert mock_store.upsert_node.called

    # 2. Column upsert (should be called for c1)
    # 3. Edge upsert (HAS_COLUMN and FOREIGN_KEY_TO)
    assert mock_store.upsert_edge.called

    # Verify introspector calls
    mock_introspector.list_table_names.assert_called_once()
    mock_introspector.get_table_def.assert_called_with("t1")
    mock_introspector.get_sample_rows.assert_called_with("t1")


@patch("ingestion.graph_hydrator.EmbeddingService")
def test_table_embedding_text_includes_normalized_hints(mock_embedding_service):
    """Ensure embedding text includes column names, normalized tokens, and synonyms."""
    mock_store = MagicMock(spec=GraphStore)
    mock_embedding_service.return_value.embed_text = MagicMock(return_value=[0.1, 0.2])
    hydrator = GraphHydrator(store=mock_store)

    table_def = TableDef(
        name="orders",
        description="order records",
        columns=[
            ColumnDef(
                name="email_addr",
                data_type="varchar",
                is_primary_key=False,
                is_nullable=True,
            ),
            ColumnDef(
                name="qty",
                data_type="integer",
                is_primary_key=False,
                is_nullable=False,
            ),
        ],
        foreign_keys=[],
    )

    hydrator._create_table_node(table_def, table_def.columns)

    call_args = hydrator.embedding_service.embed_text.call_args[0][0]
    assert "email_addr" in call_args
    assert "email addr" in call_args
    assert "email address" in call_args
    assert "quantity" in call_args


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
