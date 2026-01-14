"""Dependency injection and singleton management for ingestion services."""

from mcp_server.dal.factory import get_graph_store
from mcp_server.dal.interfaces import GraphStore


def get_ingestion_graph_store() -> GraphStore:
    """Get the singleton GraphStore instance for ingestion.

    Uses the canonical DAL factory.
    """
    return get_graph_store()
