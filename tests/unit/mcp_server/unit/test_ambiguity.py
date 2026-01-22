"""Unit tests for the AmbiguityService."""

from unittest.mock import patch

import pytest


@pytest.fixture
def mock_resolver():
    """Mock the AmbiguityResolver instance."""
    with patch("mcp_server.services.ambiguity.resolver.AmbiguityResolver") as mock:
        yield mock


@pytest.mark.asyncio
async def test_detect_ambiguity(mock_resolver):
    """Test detecting ambiguity in a query."""
    # Simple test case
    pass
