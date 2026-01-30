from fastapi.testclient import TestClient

from ui_api_gateway import app as gateway_app
from ui_api_gateway.app import MCPConnectionError, MCPTimeoutError, MCPUpstreamError

# ---------------------------------------------------------------------------
# Error Handling Tests (Phase 1 - Protocol Error Hardening)
# ---------------------------------------------------------------------------


def test_call_tool_connection_error_returns_502(monkeypatch):
    """MCP connection failures should return 502 Bad Gateway."""

    async def fake_call_tool(name, args):
        raise MCPConnectionError(
            message="Failed to connect to MCP server",
            details={"tool_name": name},
        )

    monkeypatch.setattr(gateway_app, "_call_tool", fake_call_tool)
    client = TestClient(gateway_app.app, raise_server_exceptions=False)

    resp = client.get("/interactions")
    assert resp.status_code == 502
    body = resp.json()
    assert "error" in body
    assert body["error"]["code"] == "MCP_CONNECTION_ERROR"
    assert "message" in body["error"]


def test_call_tool_timeout_error_returns_504(monkeypatch):
    """MCP timeout failures should return 504 Gateway Timeout."""

    async def fake_call_tool(name, args):
        raise MCPTimeoutError(
            message="MCP tool timed out",
            details={"tool_name": name},
        )

    monkeypatch.setattr(gateway_app, "_call_tool", fake_call_tool)
    client = TestClient(gateway_app.app, raise_server_exceptions=False)

    resp = client.get("/interactions")
    assert resp.status_code == 504
    body = resp.json()
    assert "error" in body
    assert body["error"]["code"] == "MCP_TIMEOUT"


def test_call_tool_upstream_error_returns_502(monkeypatch):
    """MCP upstream/tool failures should return 502 Bad Gateway."""

    async def fake_call_tool(name, args):
        raise MCPUpstreamError(
            message="Tool execution failed",
            details={"tool_name": name},
        )

    monkeypatch.setattr(gateway_app, "_call_tool", fake_call_tool)
    client = TestClient(gateway_app.app, raise_server_exceptions=False)

    resp = client.get("/interactions")
    assert resp.status_code == 502
    body = resp.json()
    assert "error" in body
    assert body["error"]["code"] == "MCP_UPSTREAM_ERROR"


def test_unexpected_response_format_returns_502(monkeypatch):
    """Unexpected MCP response format should return 502."""

    async def fake_call_tool(name, args):
        # Return a dict instead of expected list for list_interactions
        return {"unexpected": "format"}

    monkeypatch.setattr(gateway_app, "_call_tool", fake_call_tool)
    client = TestClient(gateway_app.app, raise_server_exceptions=False)

    resp = client.get("/interactions")
    assert resp.status_code == 502
    body = resp.json()
    assert "error" in body
    assert body["error"]["code"] == "MCP_UPSTREAM_ERROR"


def test_error_response_includes_request_id(monkeypatch):
    """Error responses should include request_id when provided."""

    async def fake_call_tool(name, args):
        raise MCPUpstreamError(message="Tool failed")

    monkeypatch.setattr(gateway_app, "_call_tool", fake_call_tool)
    client = TestClient(gateway_app.app, raise_server_exceptions=False)

    resp = client.get("/interactions", headers={"X-Request-ID": "test-req-123"})
    assert resp.status_code == 502
    body = resp.json()
    assert body["error"]["request_id"] == "test-req-123"


# ---------------------------------------------------------------------------
# Existing Tests (Functional Behavior)
# ---------------------------------------------------------------------------


def test_list_interactions_filters(monkeypatch):
    """Filter interactions by thumb/status and sort by created_at."""

    async def fake_call_tool(name, args):
        assert name == "list_interactions"
        return [
            {"id": "1", "thumb": "UP", "execution_status": "PENDING", "created_at": "2025-01-02"},
            {
                "id": "2",
                "thumb": "DOWN",
                "execution_status": "PENDING",
                "created_at": "2025-01-03",
            },
            {
                "id": "3",
                "thumb": None,
                "execution_status": "APPROVED",
                "created_at": "2025-01-04",
            },
        ]

    monkeypatch.setattr(gateway_app, "_call_tool", fake_call_tool)
    client = TestClient(gateway_app.app)

    resp = client.get("/interactions?thumb=UP&status=PENDING")
    assert resp.status_code == 200
    body = resp.json()
    assert [item["id"] for item in body] == ["1"]


def test_registry_examples_search(monkeypatch):
    """Filter registry examples by question or SQL text."""

    async def fake_call_tool(name, args):
        assert name == "list_approved_examples"
        return [
            {"question": "total sales", "sql_query": "select * from sales"},
            {"question": "refunds", "sql_query": "select * from refunds"},
        ]

    monkeypatch.setattr(gateway_app, "_call_tool", fake_call_tool)
    client = TestClient(gateway_app.app)

    resp = client.get("/registry/examples?search=refund")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["question"] == "refunds"


def test_approve_interaction_resolution_type(monkeypatch):
    """Map corrected SQL to resolution_type before calling tool."""
    calls = []

    async def fake_call_tool(name, args):
        calls.append((name, args))
        return "OK"

    monkeypatch.setattr(gateway_app, "_call_tool", fake_call_tool)
    client = TestClient(gateway_app.app)

    resp = client.post(
        "/interactions/abc/approve",
        json={"corrected_sql": "select 1", "original_sql": "select 1"},
    )
    assert resp.status_code == 200
    assert calls[-1][1]["resolution_type"] == "APPROVED_AS_IS"

    resp = client.post(
        "/interactions/abc/approve",
        json={"corrected_sql": "select 2", "original_sql": "select 1"},
    )
    assert resp.status_code == 200
    assert calls[-1][1]["resolution_type"] == "APPROVED_WITH_SQL_FIX"


def test_submit_feedback(monkeypatch):
    """Forward feedback payload to MCP tool."""

    async def fake_call_tool(name, args):
        assert name == "submit_feedback"
        assert args["interaction_id"] == "int-1"
        assert args["thumb"] == "UP"
        assert args["comment"] == "nice"
        return "OK"

    monkeypatch.setattr(gateway_app, "_call_tool", fake_call_tool)
    client = TestClient(gateway_app.app)

    resp = client.post(
        "/feedback",
        json={"interaction_id": "int-1", "thumb": "UP", "comment": "nice"},
    )
    assert resp.status_code == 200
    assert resp.json() == "OK"
