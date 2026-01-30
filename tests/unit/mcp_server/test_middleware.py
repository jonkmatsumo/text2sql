"""Tests for MCP server middleware configuration."""

from unittest.mock import MagicMock, patch

import pytest
from starlette.middleware.cors import CORSMiddleware
from starlette.testclient import TestClient

from mcp_server.middleware import (
    AUTH_EXEMPT_PATHS,
    CORS_ORIGINS,
    InternalAuthMiddleware,
    create_health_route,
    get_middleware_stack,
)


class TestInternalAuthMiddleware:
    """Tests for internal auth middleware."""

    @pytest.fixture
    def app_with_auth(self):
        """Create a test app with auth middleware."""
        from starlette.applications import Starlette
        from starlette.middleware import Middleware
        from starlette.responses import PlainTextResponse
        from starlette.routing import Route

        async def test_endpoint(request):
            return PlainTextResponse("OK")

        return Starlette(
            routes=[
                Route("/protected", test_endpoint),
                Route("/health", test_endpoint),
                Route("/messages", test_endpoint),
            ],
            middleware=[Middleware(InternalAuthMiddleware)],
        )

    def test_auth_allows_exempt_paths_without_token(self, app_with_auth, monkeypatch):
        """Auth exempt paths should work without token."""
        monkeypatch.setenv("INTERNAL_AUTH_TOKEN", "secret-token")
        client = TestClient(app_with_auth)

        # Health and messages should be allowed
        for path in AUTH_EXEMPT_PATHS:
            resp = client.get(path)
            assert resp.status_code == 200

    def test_auth_blocks_protected_path_without_token(self, app_with_auth, monkeypatch):
        """Protected paths should require token."""
        monkeypatch.setenv("INTERNAL_AUTH_TOKEN", "secret-token")
        client = TestClient(app_with_auth)

        resp = client.get("/protected")
        assert resp.status_code == 401
        assert resp.json()["error"] == "Unauthorized"

    def test_auth_allows_protected_path_with_valid_token(self, app_with_auth, monkeypatch):
        """Protected paths should work with valid token."""
        monkeypatch.setenv("INTERNAL_AUTH_TOKEN", "secret-token")
        client = TestClient(app_with_auth)

        resp = client.get("/protected", headers={"X-Internal-Token": "secret-token"})
        assert resp.status_code == 200

    def test_auth_disabled_when_no_token_configured(self, app_with_auth, monkeypatch):
        """When no token is configured, auth should be disabled."""
        monkeypatch.setenv("INTERNAL_AUTH_TOKEN", "")
        client = TestClient(app_with_auth)

        resp = client.get("/protected")
        assert resp.status_code == 200


class TestMiddlewareStack:
    """Tests for middleware stack configuration."""

    def test_middleware_stack_includes_cors(self):
        """Middleware stack should include CORS."""
        stack = get_middleware_stack()
        cors_middleware = [m for m in stack if m.cls == CORSMiddleware]
        assert len(cors_middleware) == 1

    def test_middleware_stack_includes_auth(self):
        """Middleware stack should include auth middleware."""
        stack = get_middleware_stack()
        auth_middleware = [m for m in stack if m.cls == InternalAuthMiddleware]
        assert len(auth_middleware) == 1

    def test_cors_origins_configured(self):
        """CORS should have expected origins."""
        assert "http://localhost:5173" in CORS_ORIGINS
        assert "http://localhost:3000" in CORS_ORIGINS


class TestHealthRoute:
    """Tests for health endpoint."""

    def test_health_route_exists(self):
        """Health route should be created."""
        route = create_health_route()
        assert route.path == "/health"
        assert "GET" in route.methods

    def test_health_route_returns_status(self, monkeypatch):
        """Health route should return initialization status."""
        from starlette.applications import Starlette
        from starlette.testclient import TestClient

        # Mock init_state at the source before creating the route
        mock_state = MagicMock()
        mock_state.is_ready = True
        mock_state.as_dict.return_value = {"ready": True}

        with patch("mcp_server.health.init_state", mock_state):
            app = Starlette(routes=[create_health_route()])
            client = TestClient(app)
            resp = client.get("/health")
            assert resp.status_code == 200
            assert resp.json()["ready"] is True
