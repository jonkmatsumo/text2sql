"""MCP Server entrypoint for Text 2 SQL Agent.

This module initializes the FastMCP server and registers all database tools
via the central registry.
"""

import logging
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastmcp import FastMCP
from mcp_server.tools.registry import register_all
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.starlette import StarletteInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from common.config.env import get_env_int, get_env_str
from dal.database import Database

logger = logging.getLogger(__name__)

# OTEL Setup
OTEL_EXPORTER_OTLP_ENDPOINT = get_env_str(
    "OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317"
)
OTEL_SERVICE_NAME = get_env_str("OTEL_SERVICE_NAME", "text2sql-mcp")


def setup_telemetry():
    """Initialize OTEL SDK for MCP Server."""
    resource = Resource.create({SERVICE_NAME: OTEL_SERVICE_NAME})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=OTEL_EXPORTER_OTLP_ENDPOINT)
    processor = BatchSpanProcessor(exporter)
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
    logger.info(f"OTEL initialized for MCP Server: {OTEL_SERVICE_NAME}")


setup_telemetry()

# Load environment variables
load_dotenv()


@asynccontextmanager
async def lifespan(app):
    """Lifespan context manager for database connection pool.

    This ensures the database pool is created in the same event loop
    as the server, avoiding "Event loop is closed" errors.
    """
    # Startup: Initialize database connection pool
    await Database.init()

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

    except SystemExit:
        raise  # Re-raise for hard failure (validation failed)
    except Exception as e:
        logger.error(f"Startup validation failed unexpectedly: {e}")
        raise RuntimeError("Query-target schema validation failed") from e

    # Initialize NLP patterns from DB
    try:
        from mcp_server.services.canonicalization.spacy_pipeline import CanonicalizationService

        service = CanonicalizationService.get_instance()
        await service.reload_patterns()
    except Exception as e:
        print(f"Warning: Failed to load NLP patterns: {e}")

    # Maintenance: Prune legacy cache entries
    try:
        from mcp_server.services.cache.service import prune_legacy_entries

        count = await prune_legacy_entries()
        if count > 0:
            print(f"🧹 Pruned {count} legacy cache entries on startup")
    except Exception as e:
        print(f"Warning: Cache pruning failed: {e}")

    # Check if schema_embeddings table is empty and try to index
    # This is optional - server should still work without it
    try:
        from mcp_server.services.rag import index_all_tables

        async with Database.get_connection() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM public.schema_embeddings")
            if count == 0:
                print("Schema embeddings table is empty. Starting indexing...")
                await index_all_tables()
    except Exception as e:
        print(f"Warning: Index check or indexing failed: {e}")

    yield

    # Shutdown: Close database connection pool
    await Database.close()


# Initialize FastMCP Server with dependencies
mcp = FastMCP("text2sql-agent", lifespan=lifespan)

# Register all tools via the central registry
register_all(mcp)


if __name__ == "__main__":

    # Respect transport and host/port from environment for containerized use
    transport = get_env_str("MCP_TRANSPORT", "stdio").lower()
    host = get_env_str("MCP_HOST", "0.0.0.0")
    port = get_env_int("MCP_PORT", 8000)

    if transport in ("sse", "http", "streamable-http"):
        # We standardize on sse transport to be compatible with langchain-mcp-adapters
        # which does not yet support the session requirements of streamable-http.
        print(f"🚀 Starting MCP server in sse mode on {host}:{port}/messages")

        # Access the underlying starlette app to instrument with OTEL
        # FastMCP usually stores the app in mcp._app when using SSE
        try:
            starlette_app = mcp.get_app()
            StarletteInstrumentor().instrument_app(starlette_app)
            print("✅ Starlette app instrumented for OTEL")
        except Exception as e:
            print(f"Warning: Could not instrument Starlette app: {e}")

        mcp.run(transport="sse", host=host, port=port, path="/messages")
    else:
        mcp.run(transport="stdio")
