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
        from unittest.mock import MagicMock

        mock_result = MagicMock()
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
        from unittest.mock import MagicMock

        mock_result = MagicMock()
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
        from unittest.mock import MagicMock

        mock_result = MagicMock()
        mock_result.model_dump.return_value = {"examples": []}
        mock_service.return_value = mock_result

        await handler(query="test", tenant_id=1, limit=3, enable_fallback=False)

        mock_service.assert_called_once_with(
            question="test", tenant_id=1, limit=3, enable_fallback=False
        )


@pytest.mark.asyncio
async def test_recommend_examples_handler_includes_explanation():
    """Test that the handler includes the explanation field in the output."""
    with patch(
        "mcp_server.services.recommendation.service.RecommendationService.recommend_examples",
        new_callable=AsyncMock,
    ) as mock_service:
        from mcp_server.services.recommendation.explanation import RecommendationExplanation
        from mcp_server.services.recommendation.interface import RecommendationResult

        mock_explanation = RecommendationExplanation(selection_summary={"total_candidates": 5})
        mock_result = RecommendationResult(examples=[], explanation=mock_explanation)
        mock_service.return_value = mock_result

        import json

        response_json = await handler(query="test", tenant_id=1, limit=3)
        response = json.loads(response_json)["result"]

        assert "explanation" in response
        assert response["explanation"]["selection_summary"]["total_candidates"] == 5


@pytest.mark.asyncio
async def test_recommend_examples_handler_includes_safety_info():
    """Test that the handler includes safety filtering info in the explanation."""
    with patch(
        "mcp_server.services.recommendation.service.RecommendationService.recommend_examples",
        new_callable=AsyncMock,
    ) as mock_service:
        from mcp_server.services.recommendation.explanation import (
            FilteringExplanation,
            RecommendationExplanation,
        )
        from mcp_server.services.recommendation.interface import RecommendationResult

        mock_explanation = RecommendationExplanation(
            filtering=FilteringExplanation(safety_removed=2)
        )
        mock_result = RecommendationResult(examples=[], explanation=mock_explanation)
        mock_service.return_value = mock_result

        import json

        response_json = await handler(query="test", tenant_id=1, limit=3)
        response = json.loads(response_json)["result"]

        assert "explanation" in response
        assert response["explanation"]["filtering"]["safety_removed"] == 2


@pytest.mark.asyncio
async def test_recommend_examples_requires_tenant_id():
    """Verify that recommend_examples requires tenant_id."""
    import json

    response_json = await handler(query="test", tenant_id=None)
    response = json.loads(response_json)
    assert "error" in response
    assert "Tenant ID is required" in response["error"]
    assert response["error_category"] == "invalid_request"
