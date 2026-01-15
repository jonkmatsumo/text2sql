from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from common.interfaces import GraphStore
from ingestion.sync_engine import SyncEngine
from schema import ColumnDef, TableDef


def test_sync_engine_init():
    """Test initialization."""
    mock_store = MagicMock(spec=GraphStore)
    mock_introspector = MagicMock()

    # Introspector is now required argument
    engine = SyncEngine(store=mock_store, introspector=mock_introspector)
    assert engine.introspector == mock_introspector


@pytest.mark.asyncio
async def test_get_live_schema():
    """Test get_live_schema uses introspector and formats correctly."""
    mock_introspector = MagicMock()
    mock_introspector.list_table_names = AsyncMock(return_value=["t1"])

    c1 = ColumnDef(name="c1", data_type="INTEGER", is_primary_key=True, is_nullable=False)
    t1_def = TableDef(name="t1", columns=[c1], description="desc")
    mock_introspector.get_table_def = AsyncMock(return_value=t1_def)

    mock_store = MagicMock(spec=GraphStore)
    engine = SyncEngine(store=mock_store, introspector=mock_introspector)
    schema = await engine.get_live_schema()

    assert "t1" in schema["tables"]
    assert "c1" in schema["tables"]["t1"]
    col_info = schema["tables"]["t1"]["c1"]
    assert col_info["type"] == "INTEGER"
    assert col_info["primary_key"] is True

    mock_introspector.list_table_names.assert_called_once()
    mock_introspector.get_table_def.assert_called_with("t1")
