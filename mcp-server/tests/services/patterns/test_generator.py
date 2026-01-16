"""Unit tests for pattern generator."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ingestion.patterns.generator import enrich_values_with_llm, generate_entity_patterns


@pytest.mark.asyncio
async def test_enrich_values_with_llm_success():
    """Test successful LLM enrichment."""
    mock_client = AsyncMock()
    mock_response = MagicMock()
    mock_response.choices = [
        MagicMock(message=MagicMock(content='[{"pattern": "syn1", "id": "VAL"}]'))
    ]
    mock_client.chat.completions.create.return_value = mock_response

    patterns = await enrich_values_with_llm(mock_client, "LABEL", ["VAL"])

    assert len(patterns) == 1
    assert patterns[0]["pattern"] == "syn1"
    assert patterns[0]["label"] == "LABEL"
    assert patterns[0]["id"] == "VAL"


@pytest.mark.asyncio
async def test_enrich_values_with_llm_no_client():
    """Test enrichment with no client returns empty."""
    patterns = await enrich_values_with_llm(None, "LABEL", ["VAL"])
    assert patterns == []


@pytest.mark.asyncio
async def test_generate_entity_patterns():
    """Test the full generation pipeline with mocks."""
    from schema import ColumnDef, TableDef

    # Mock Database Connection
    mock_conn = AsyncMock()
    # Mock value scan fetch
    # It might be called for columns that match heuristics.
    # We will define one column "status" that triggers scan.
    # Return value for SELECT DISTINCT ...
    mock_conn.fetch.return_value = [["Active"]]

    mock_db_ctx = AsyncMock()
    mock_db_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_db_ctx.__aexit__ = AsyncMock(return_value=None)

    # Mock Introspector
    mock_introspector = AsyncMock()
    mock_introspector.list_table_names.return_value = ["users"]
    mock_introspector.get_table_def.return_value = TableDef(
        name="users",
        columns=[
            ColumnDef(name="id", data_type="integer", is_nullable=False),
            ColumnDef(name="status", data_type="text", is_nullable=True),  # Should trigger scan
        ],
        foreign_keys=[],
        description="User table",
    )

    # Mock OpenAI Client
    mock_client = AsyncMock()
    mock_llm_resp = MagicMock()
    mock_llm_resp.choices = [
        MagicMock(message=MagicMock(content='[{"pattern": "synonym", "id": "Active"}]'))
    ]
    mock_client.chat.completions.create.return_value = mock_llm_resp

    with patch("dal.database.Database.get_connection", return_value=mock_db_ctx), patch(
        "dal.database.Database.get_schema_introspector",
        return_value=mock_introspector,
    ), patch("ingestion.patterns.generator.get_openai_client", return_value=mock_client):

        patterns = await generate_entity_patterns()

        # Verify introspector usage
        mock_introspector.list_table_names.assert_called_once()
        mock_introspector.get_table_def.assert_called_with("users")

        # Verify Value Scan occurred (fetch called)
        assert mock_conn.fetch.called

        # Verify Patterns
        # 1. Table
        assert any(p["label"] == "TABLE" and p["pattern"] == "users" for p in patterns)

        # 2. Columns
        assert any(p["label"] == "COLUMN" and p["pattern"] == "status" for p in patterns)
        assert any(p["label"] == "COLUMN" and p["pattern"] == "id" for p in patterns)

        # 3. Values (from scan)
        # Should have label STATUS (uppercase column name)
        assert any(p["label"] == "STATUS" and p["pattern"] == "active" for p in patterns)

        # 4. LLM Enrichment (mocked synonym)
        assert any(p["label"] == "STATUS" and p["pattern"] == "synonym" for p in patterns)
