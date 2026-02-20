"""MCP Server Middleware Configuration.

This module provides middleware for the MCP server, replacing the previous
monkey-patched approach with a cleaner factory-based pattern.

Middleware added:
- CORS for cross-origin requests from UI
- Internal auth token verification
- OpenTelemetry instrumentation
"""

import logging
from typing import Callable, List

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.cors import CORSMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from common.config.env import get_env_str
from common.observability.context import request_id_var, run_id_var
from mcp_server.utils.request_auth_context import (
    reset_internal_auth_verified,
    set_internal_auth_verified,
)

logger = logging.getLogger(__name__)

# CORS allowed origins for UI access
CORS_ORIGINS = [
    "http://localhost:3333",
    "http://127.0.0.1:3333",
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://localhost:8080",
    "http://127.0.0.1:8080",
]

# Paths that don't require authentication
AUTH_EXEMPT_PATHS = ["/health", "/messages"]


class RunIdMiddleware(BaseHTTPMiddleware):
    """Middleware to propagate X-Run-ID header to context and spans."""

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Extract run_id from header and set in context/span."""
        run_id = request.headers.get("X-Run-ID")
        request_id = request.headers.get("X-Request-ID") or run_id
        run_token = None
        request_token = None
        if run_id:
            run_token = run_id_var.set(run_id)
            from opentelemetry import trace

            span = trace.get_current_span()
            if span.is_recording():
                span.set_attribute("run_id", run_id)
        if request_id:
            request_token = request_id_var.set(request_id)
            from opentelemetry import trace

            span = trace.get_current_span()
            if span.is_recording():
                span.set_attribute("mcp.request_id", request_id)

        try:
            return await call_next(request)
        finally:
            if run_token:
                run_id_var.reset(run_token)
            if request_token:
                request_id_var.reset(request_token)


class InternalAuthMiddleware(BaseHTTPMiddleware):
    """Middleware to verify internal authentication token.

    Checks X-Internal-Token header against INTERNAL_AUTH_TOKEN env var.
    Allows /health and /messages paths without authentication.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Verify auth token on requests to protected paths."""
        token = get_env_str("INTERNAL_AUTH_TOKEN", "")
        verified_internal_auth = False
        if token and request.url.path not in AUTH_EXEMPT_PATHS:
            request_token = request.headers.get("X-Internal-Token")
            if request_token != token:
                return JSONResponse({"error": "Unauthorized"}, status_code=401)
            verified_internal_auth = True

        auth_token = set_internal_auth_verified(verified_internal_auth)
        try:
            return await call_next(request)
        finally:
            reset_internal_auth_verified(auth_token)


def get_middleware_stack() -> List[Middleware]:
    """Build the middleware stack for the MCP server.

    Returns:
        List of Starlette Middleware instances in application order.
    """
    return [
        Middleware(
            CORSMiddleware,
            allow_origins=CORS_ORIGINS,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        ),
        Middleware(RunIdMiddleware),
        Middleware(InternalAuthMiddleware),
    ]


def instrument_app(app: Starlette) -> None:
    """Apply OpenTelemetry instrumentation to the Starlette app.

    Args:
        app: Starlette application to instrument.
    """
    try:
        from opentelemetry.instrumentation.starlette import StarletteInstrumentor

        StarletteInstrumentor().instrument_app(app)
        logger.info("OTEL instrumentation applied to MCP server")
    except ImportError:
        logger.warning("OpenTelemetry instrumentation not available")
    except Exception as e:
        logger.error("Failed to apply OTEL instrumentation: %s", e)


def create_health_route() -> Route:
    """Create the /health endpoint route.

    Returns:
        Starlette Route for health checks.
    """
    from mcp_server.health import init_state

    async def health_handler(request: Request) -> JSONResponse:
        """Health/readiness endpoint reflecting initialization status."""
        status = init_state.as_dict()
        http_status = 200 if init_state.is_ready else 503
        return JSONResponse(status, status_code=http_status)

    return Route("/health", health_handler, methods=["GET"])


def wrap_mcp_app(mcp_app: Starlette) -> Starlette:
    """Wrap MCP app with middleware and additional routes.

    This is the supported way to add middleware and routes to a FastMCP server
    without monkey-patching. It creates a new Starlette app that composes
    the MCP app with our middleware stack.

    Args:
        mcp_app: The base MCP Starlette application.

    Returns:
        Wrapped Starlette application with middleware and health endpoint.
    """
    from starlette.routing import Mount

    # Create a new app with our middleware
    wrapped = Starlette(
        routes=[
            create_health_route(),
            Mount("/", app=mcp_app),
        ],
        middleware=get_middleware_stack(),
    )

    # Apply OTEL instrumentation
    instrument_app(wrapped)

    return wrapped
