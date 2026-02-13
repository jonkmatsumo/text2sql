from starlette.middleware import Middleware
from starlette.responses import JSONResponse
from starlette.testclient import TestClient

from common.observability.context import run_id_var
from mcp_server.middleware import RunIdMiddleware


class TestRunIdMiddleware:
    """Tests for RunIdMiddleware."""

    def test_run_id_propagation(self):
        """Verify that X-Run-ID header is propagated to context."""

        async def endpoint(request):
            return JSONResponse({"run_id": run_id_var.get()})

        from starlette.applications import Starlette
        from starlette.routing import Route

        app = Starlette(routes=[Route("/", endpoint)], middleware=[Middleware(RunIdMiddleware)])

        client = TestClient(app)

        # Test without header
        resp = client.get("/")
        assert resp.json()["run_id"] is None

        # Test with header
        resp = client.get("/", headers={"X-Run-ID": "test-run-123"})
        assert resp.json()["run_id"] == "test-run-123"
