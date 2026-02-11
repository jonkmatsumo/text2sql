"""MCP Server entrypoint for Text 2 SQL Agent.

This module initializes the FastMCP server and registers all database tools
via the central registry.
"""

import logging
import os
import sys
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastmcp import FastMCP
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor

from common.config.env import get_env_bool, get_env_int, get_env_str
from dal.database import Database
from mcp_server.tools.registry import register_all

# Configure logging at the start
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# OTEL Setup
OTEL_EXPORTER_OTLP_ENDPOINT = get_env_str(
    "OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317"
)
OTEL_SERVICE_NAME = get_env_str("OTEL_SERVICE_NAME", "text2sql-mcp")


def setup_telemetry():
    """Initialize OTEL SDK for MCP Server."""
    resource = Resource.create({SERVICE_NAME: OTEL_SERVICE_NAME})
    provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(provider)

    if not get_env_bool("OTEL_WORKER_ENABLED", True):
        logger.info("OTEL initialized without exporter (OTEL_WORKER_ENABLED=false)")
        return

    if get_env_bool("OTEL_DISABLE_EXPORTER", False):
        logger.info("OTEL initialized without exporter (OTEL_DISABLE_EXPORTER=true)")
        return

    worker_required = get_env_bool("OTEL_WORKER_REQUIRED", False)
    is_pytest = "PYTEST_CURRENT_TEST" in os.environ
    exporter_mode_default = "in_memory" if is_pytest else "otlp"
    exporter_mode = (
        (get_env_str("OTEL_TEST_EXPORTER", exporter_mode_default) or exporter_mode_default)
        .strip()
        .lower()
    )

    try:
        if exporter_mode == "none":
            logger.info("OTEL initialized without exporter (OTEL_TEST_EXPORTER=none)")
            return
        if exporter_mode == "in_memory":
            from common.observability.in_memory_exporter import get_or_create_span_exporter

            exporter = get_or_create_span_exporter("mcp")
            provider.add_span_processor(SimpleSpanProcessor(exporter))
            logger.info("OTEL initialized with in-memory exporter (scope=mcp)")
            return

        exporter = OTLPSpanExporter(endpoint=OTEL_EXPORTER_OTLP_ENDPOINT)
        processor = BatchSpanProcessor(exporter)
        provider.add_span_processor(processor)
        logger.info(f"OTEL initialized for MCP Server: {OTEL_SERVICE_NAME}")
    except Exception as exc:
        if worker_required:
            raise RuntimeError(
                "Failed to initialize OTEL exporter while OTEL_WORKER_REQUIRED=true. "
                "Set OTEL_WORKER_REQUIRED=false for degraded operation."
            ) from exc
        logger.exception("Failed to initialize MCP OTEL exporter; continuing degraded: %s", exc)


setup_telemetry()


@asynccontextmanager
async def lifespan(app):
    """Lifespan context manager for database connection pool.

    This ensures the database pool is created in the same event loop
    as the server, avoiding "Event loop is closed" errors.

    Tracks initialization status via InitializationState for health endpoint.
    """
    from mcp_server.health import init_state

    init_state.start()

    # Startup: Validate runtime configuration combinations (required)
    try:
        from common.config.sanity import validate_runtime_configuration

        validate_runtime_configuration()
        init_state.record_success("config_sanity", required=True)
    except Exception as e:
        logger.exception("Runtime configuration sanity check failed")
        init_state.record_failure("config_sanity", e, required=True)
        raise RuntimeError("Runtime configuration sanity check failed") from e

    # Startup: Initialize database connection pool (required)
    try:
        await Database.init()
        init_state.record_success("database", required=True)
    except Exception as e:
        logger.exception("Database initialization failed")
        init_state.record_failure("database", e, required=True)
        # Database is critical - we can still start but mark not ready

    # P0: Fail-fast validation — ensure query-target schema exists
    # This MUST run before any other startup logic that depends on schema
    try:
        from mcp_server.services.seeding.validation import (
            run_mcp_startup_validation,
            warn_if_quality_files_missing,
        )

        async with Database.get_connection() as conn:
            await run_mcp_startup_validation(conn)

        # P2: Warn about optional quality files (non-fatal)
        warn_if_quality_files_missing()
        init_state.record_success("schema_validation", required=True)

    except SystemExit:
        raise  # Re-raise for hard failure (validation failed)
    except Exception as e:
        logger.exception("Startup validation failed unexpectedly")
        init_state.record_failure("schema_validation", e, required=True)
        raise RuntimeError("Query-target schema validation failed") from e

    # P3: Emit few-shot registry status (quality observability)
    try:
        from mcp_server.services.registry import RegistryService

        examples = await RegistryService.list_examples(tenant_id=1, limit=1000)
        examples_count = len(examples)
        logger.info(
            "event=fewshot_registry_status examples_count=%d "
            "examples_source=query_pairs loaded=%s",
            examples_count,
            examples_count > 0,
        )
        if examples_count == 0:
            logger.warning(
                "event=fewshot_registry_empty "
                "impact='Generation quality may be degraded without few-shot examples'"
            )
        init_state.record_success("registry_status", required=False)
    except Exception as e:
        logger.warning("Failed to check registry status: %s", e)
        init_state.record_failure("registry_status", e, required=False)

    # Initialize NLP patterns from DB (optional - fallback behavior exists)
    try:
        from mcp_server.services.canonicalization.spacy_pipeline import CanonicalizationService

        service = CanonicalizationService.get_instance()
        await service.reload_patterns()
        init_state.record_success("nlp_patterns", required=False)
    except Exception as e:
        logger.exception("NLP patterns initialization failed")
        init_state.record_failure("nlp_patterns", e, required=False)

    # Maintenance: Prune legacy cache entries (optional)
    try:
        from mcp_server.services.cache.service import prune_legacy_entries

        count = await prune_legacy_entries()
        if count > 0:
            logger.info("Pruned %d legacy cache entries on startup", count)
        init_state.record_success("cache_pruning", required=False)
    except Exception as e:
        logger.exception("Cache pruning failed")
        init_state.record_failure("cache_pruning", e, required=False)

    # Check if schema_embeddings table is empty and try to index (optional)
    try:
        from mcp_server.services.rag import index_all_tables

        async with Database.get_connection() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM public.schema_embeddings")
            if count == 0:
                logger.info("Schema embeddings table is empty. Starting indexing...")
                await index_all_tables()
        init_state.record_success("schema_embeddings", required=False)
    except Exception as e:
        logger.exception("Schema embeddings indexing failed")
        init_state.record_failure("schema_embeddings", e, required=False)

    init_state.complete()

    if init_state.is_ready:
        logger.info("MCP server initialization complete - ready")
    else:
        failed = [c.name for c in init_state.failed_checks]
        logger.warning("MCP server initialization complete with failures: %s", failed)

    yield

    # Shutdown: Close database connection pool
    await Database.close()


# Initialize FastMCP Server with dependencies
mcp = FastMCP("text2sql-agent", lifespan=lifespan)

# Register all tools via the central registry
register_all(mcp)


# ---------------------------------------------------------------------------
# Middleware Integration (replaces previous monkey-patching approach)
# ---------------------------------------------------------------------------
# FastMCP exposes http_app as a factory. We wrap it to add middleware cleanly.
# See mcp_server/middleware.py for the middleware stack.

try:
    from mcp_server.middleware import create_health_route, get_middleware_stack, instrument_app

    if hasattr(mcp, "http_app"):
        _original_factory = mcp.http_app

        def _wrapped_factory(*args, **kwargs):
            """Create MCP app with middleware via factory wrapper.

            This approach:
            - Uses a factory wrapper instead of direct patching
            - Adds middleware through Starlette's standard mechanism
            - Is documented and maintainable
            - Avoids modifying FastMCP internals directly
            """
            app = _original_factory(*args, **kwargs)
            try:
                # Add health endpoint
                app.routes.insert(0, create_health_route())

                # Add middleware (CORS, Auth)
                for middleware in reversed(get_middleware_stack()):
                    app.add_middleware(middleware.cls, **middleware.kwargs)

                # Apply OTEL instrumentation
                instrument_app(app)

                logger.info("MCP app configured with middleware and health endpoint")
            except Exception as e:
                logger.error("Failed to configure MCP app middleware: %s", e)
            return app

        mcp.http_app = _wrapped_factory
        logger.info("MCP http_app factory wrapped with middleware support")
    else:
        logger.warning("mcp.http_app not found - middleware not configured")

except ImportError as e:
    logger.warning("Could not import middleware module: %s", e)
except Exception as e:
    logger.error("Error configuring MCP middleware: %s", e)


if __name__ == "__main__":

    # Respect transport and host/port from environment for containerized use
    transport = get_env_str("MCP_TRANSPORT", "stdio").lower()
    host = get_env_str("MCP_HOST", "0.0.0.0")
    port = get_env_int("MCP_PORT", 8000)

    if transport in ("sse", "http", "streamable-http"):
        print(
            f"🚀 Starting MCP server in sse mode on {host}:{port}/messages",
            file=sys.stderr,
            flush=True,
        )
        mcp.run(transport="sse", host=host, port=port, path="/messages")
    else:
        mcp.run(transport="stdio")
