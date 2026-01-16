"""Dependency injection and singleton management for ingestion services."""

from common.interfaces import GraphStore
from dal.factory import get_graph_store


def get_ingestion_graph_store() -> GraphStore:
    """Get the singleton GraphStore instance for ingestion.

    Uses the canonical DAL factory.
    """
    return get_graph_store()
