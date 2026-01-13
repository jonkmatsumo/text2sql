"""Unit tests for recommend_examples tool."""

from unittest.mock import AsyncMock, patch

import pytest
from mcp_server.tools.recommend_examples import handler


@pytest.mark.asyncio
async def test_recommend_examples_handler_fallback_none():
    """Test handler when enable_fallback is None (default)."""
    with patch(
        "mcp_server.services.recommendation.service.RecommendationService.recommend_examples",
        new_callable=AsyncMock,
    ) as mock_service:
        mock_result = AsyncMock()
        mock_result.model_dump.return_value = {"examples": []}
        mock_service.return_value = mock_result

        await handler(query="test", tenant_id=1, limit=3, enable_fallback=None)

        # Should NOT pass enable_fallback if it is None, relying on service default
        mock_service.assert_called_once_with(question="test", tenant_id=1, limit=3)


@pytest.mark.asyncio
async def test_recommend_examples_handler_fallback_true():
    """Test handler when enable_fallback is True."""
    with patch(
        "mcp_server.services.recommendation.service.RecommendationService.recommend_examples",
        new_callable=AsyncMock,
    ) as mock_service:
        mock_result = AsyncMock()
        mock_result.model_dump.return_value = {"examples": []}
        mock_service.return_value = mock_result

        await handler(query="test", tenant_id=1, limit=3, enable_fallback=True)

        mock_service.assert_called_once_with(
            question="test", tenant_id=1, limit=3, enable_fallback=True
        )


@pytest.mark.asyncio
async def test_recommend_examples_handler_fallback_false():
    """Test handler when enable_fallback is False."""
    with patch(
        "mcp_server.services.recommendation.service.RecommendationService.recommend_examples",
        new_callable=AsyncMock,
    ) as mock_service:
        mock_result = AsyncMock()
        mock_result.model_dump.return_value = {"examples": []}
        mock_service.return_value = mock_result

        await handler(query="test", tenant_id=1, limit=3, enable_fallback=False)

        mock_service.assert_called_once_with(
            question="test", tenant_id=1, limit=3, enable_fallback=False
        )
