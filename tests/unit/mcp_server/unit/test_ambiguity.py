"""Unit tests for the AmbiguityService."""

import pytest


@pytest.fixture
def mock_resolver(mocker):
    """Mock the AmbiguityResolver instance."""
    return mocker.patch("mcp_server.services.ambiguity.resolver.AmbiguityResolver")


@pytest.mark.asyncio
async def test_detect_ambiguity(mock_resolver):
    """Test detecting ambiguity in a query."""
    # Simple test case
    pass
