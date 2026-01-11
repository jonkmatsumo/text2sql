from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from mcp_server.models import ColumnDef, TableDef
from mcp_server.services.ingestion.sync_engine import SyncEngine


@patch("mcp_server.services.ingestion.sync_engine.get_schema_introspector")
@patch("mcp_server.services.ingestion.sync_engine.MemgraphStore")
def test_sync_engine_init(mock_memgraph_store_cls, mock_get_introspector):
    """Test initialization."""
    SyncEngine()
    mock_get_introspector.assert_called_once()
    mock_memgraph_store_cls.assert_called_once()


@patch("mcp_server.services.ingestion.sync_engine.get_schema_introspector")
@patch("mcp_server.services.ingestion.sync_engine.MemgraphStore")
@pytest.mark.asyncio
async def test_get_live_schema(mock_memgraph_store_cls, mock_get_introspector):
    """Test get_live_schema uses introspector and formats correctly."""
    mock_introspector = MagicMock()

    mock_introspector.list_table_names = AsyncMock(return_value=["t1"])

    c1 = ColumnDef(name="c1", data_type="INTEGER", is_primary_key=True, is_nullable=False)
    t1_def = TableDef(name="t1", columns=[c1], description="desc")
    mock_introspector.get_table_def = AsyncMock(return_value=t1_def)

    mock_get_introspector.return_value = mock_introspector

    engine = SyncEngine()
    schema = await engine.get_live_schema()

    assert "t1" in schema["tables"]
    assert "c1" in schema["tables"]["t1"]
    col_info = schema["tables"]["t1"]["c1"]
    assert col_info["type"] == "INTEGER"
    assert col_info["primary_key"] is True

    mock_introspector.list_table_names.assert_called_once()
    mock_introspector.get_table_def.assert_called_with("t1")
