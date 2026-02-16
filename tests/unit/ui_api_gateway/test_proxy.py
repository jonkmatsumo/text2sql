from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from ui_api_gateway.app import app


@pytest.fixture
def client():
    """Create a test client for the UI API gateway."""
    return TestClient(app)


@pytest.mark.asyncio
async def test_proxy_agent_stream(client):
    """Test the UI Gateway proxy streaming endpoint."""
    # Mock httpx.AsyncClient
    mock_events = [b"event: startup\n\n", b"event: progress\n\n"]

    class MockResponse:
        status_code = 200

        async def aiter_bytes(self):
            for event in mock_events:
                yield event

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    class MockClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

        def stream(self, *args, **kwargs):
            return MockResponse()

    with patch("httpx.AsyncClient", side_effect=MockClient):
        with client.stream(
            "POST", "/agent/run/stream", json={"question": "test", "tenant_id": 1}
        ) as response:
            assert response.status_code == 200
            content = b"".join(response.iter_bytes())
            assert b"event: startup" in content
            assert b"event: progress" in content
