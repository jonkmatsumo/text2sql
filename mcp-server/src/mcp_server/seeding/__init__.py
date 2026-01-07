"""Seeding utilities for database initialization.

Two-phase seeding architecture:
- Phase 1: SQL scripts insert static data (database init)
- Phase 2: Python generates embeddings (MCP server startup)
"""

from mcp_server.seeding.examples import generate_missing_embeddings

__all__ = ["generate_missing_embeddings"]
