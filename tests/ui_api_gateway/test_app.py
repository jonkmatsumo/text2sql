from fastapi.testclient import TestClient

from ui_api_gateway import app as gateway_app


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
