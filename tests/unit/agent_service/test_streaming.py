import json
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from agent_service.app import app


@pytest.fixture
def client():
    """Create a test client for the agent service."""
    return TestClient(app)


@pytest.mark.asyncio
async def test_agent_run_stream(client):
    """Test the agent streaming endpoint with mocked graph execution."""
    # Mock run_agent_with_tracing_stream
    mock_events = [
        {"event": "progress", "data": {"phase": "plan", "timestamp": 1234567890}},
        {"event": "final", "data": {"sql": "SELECT * FROM table", "result": [{"col": "val"}]}},
    ]

    async def mock_stream(*args, **kwargs):
        for event in mock_events:
            yield event

    with patch("agent.graph.run_agent_with_tracing_stream", side_effect=mock_stream):
        # We need to use async client or just test the endpoint logic if possible
        # TestClient is synchronous, but app is async.
        # For streaming, TestClient supports stream=True

        payload = {"question": "test question", "tenant_id": 1, "thread_id": "test_thread"}

        with client.stream("POST", "/agent/run/stream", json=payload) as response:
            assert response.status_code == 200
            lines = list(response.iter_lines())

            # Check for startup event
            assert "event: startup" in lines[0]

            # Check for progress event
            # lines are strings. SSE format is event: ... data: ...

            full_text = "\n".join(lines)
            assert "event: progress" in full_text
            assert '{"phase": "plan"' in full_text

            assert "event: result" in full_text
            assert "SELECT * FROM table" in full_text


@pytest.mark.asyncio
async def test_agent_run_stream_populates_error_code_for_error_payload(client):
    """Streamed final payloads with errors should include canonical error_code."""
    mock_events = [
        {
            "event": "final",
            "data": {
                "error": 'relation "users" does not exist',
                "error_category": "syntax",
            },
        },
    ]

    async def mock_stream(*args, **kwargs):
        del args, kwargs
        for event in mock_events:
            yield event

    with patch("agent.graph.run_agent_with_tracing_stream", side_effect=mock_stream):
        payload = {"question": "test question", "tenant_id": 1, "thread_id": "test_thread"}
        with client.stream("POST", "/agent/run/stream", json=payload) as response:
            assert response.status_code == 200
            lines = list(response.iter_lines())

    data_lines = [line for line in lines if line.startswith("data: ")]
    assert data_lines
    final_payload = json.loads(data_lines[-1][6:])
    assert final_payload["error_code"] == "DB_SYNTAX_ERROR"
    assert "users" not in final_payload["error"].lower()
