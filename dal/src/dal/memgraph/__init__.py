"""Memgraph DAL Implementations.

This package contains the concrete implementations of DAL interfaces for Memgraph/Neo4j.
"""

from .graph_store import MemgraphStore

__all__ = ["MemgraphStore"]
