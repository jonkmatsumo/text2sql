"""Unit tests for manage_pin_rules tool."""

import json
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from mcp_server.tools.manage_pin_rules import handler


@pytest.fixture
def mock_store():
    """Mock the PostgresPinnedRecommendationStore."""
    with patch("mcp_server.tools.manage_pin_rules.PostgresPinnedRecommendationStore") as mock_cls:
        instance = mock_cls.return_value
        instance.list_rules = AsyncMock()
        instance.create_rule = AsyncMock()
        instance.update_rule = AsyncMock()
        instance.delete_rule = AsyncMock()
        yield instance


@pytest.mark.asyncio
async def test_list_rules(mock_store):
    """Verify list_rules calls the store correctly."""
    mock_rule = MagicMock()
    mock_rule.id = uuid4()
    mock_rule.tenant_id = 1
    mock_rule.match_type = "exact"
    mock_rule.match_value = "foo"
    mock_rule.registry_example_ids = ["ex1"]
    mock_rule.priority = 10
    mock_rule.enabled = True
    mock_rule.created_at = None
    mock_rule.updated_at = None

    mock_store.list_rules.return_value = [mock_rule]

    raw_result = await handler("list", tenant_id=1)
    result = json.loads(raw_result)["result"]

    assert len(result) == 1
    assert result[0]["id"] == str(mock_rule.id)
    assert result[0]["match_value"] == "foo"
    mock_store.list_rules.assert_called_once_with(1)


@pytest.mark.asyncio
async def test_upsert_create_rule(mock_store):
    """Verify upsert operation for creating a rule."""
    mock_rule = MagicMock()
    mock_rule.id = uuid4()
    mock_rule.tenant_id = 1
    mock_rule.match_type = "contains"
    mock_rule.match_value = "bar"
    mock_rule.registry_example_ids = ["ex2"]
    mock_rule.priority = 5
    mock_rule.enabled = True
    mock_rule.created_at = None
    mock_rule.updated_at = None

    mock_store.create_rule.return_value = mock_rule

    raw_result = await handler(
        "upsert",
        tenant_id=1,
        match_type="contains",
        match_value="bar",
        registry_example_ids=["ex2"],
    )
    result = json.loads(raw_result)["result"]

    assert result["id"] == str(mock_rule.id)
    assert result["match_type"] == "contains"
    mock_store.create_rule.assert_called_once()  # Args check omitted for brevity


@pytest.mark.asyncio
async def test_upsert_update_rule(mock_store):
    """Verify upsert operation for updating a rule."""
    rule_id = str(uuid4())
    mock_rule = MagicMock()
    mock_rule.id = rule_id
    mock_rule.tenant_id = 1
    mock_rule.match_type = "exact"
    mock_rule.match_value = "baz"
    mock_rule.registry_example_ids = []
    mock_rule.priority = 0
    mock_rule.enabled = False
    mock_rule.created_at = None
    mock_rule.updated_at = None

    mock_store.update_rule.return_value = mock_rule

    raw_result = await handler("upsert", tenant_id=1, rule_id=rule_id, enabled=False)
    result = json.loads(raw_result)["result"]

    assert result["id"] == rule_id
    assert result["enabled"] is False
    mock_store.update_rule.assert_called_once()


@pytest.mark.asyncio
async def test_delete_rule(mock_store):
    """Verify delete_rule calls the store correctly."""
    rule_id = str(uuid4())
    mock_store.delete_rule.return_value = True

    raw_result = await handler("delete", tenant_id=1, rule_id=rule_id)
    result = json.loads(raw_result)["result"]

    assert result["success"] is True
    mock_store.delete_rule.assert_called_once()
