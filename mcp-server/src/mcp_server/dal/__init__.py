"""Data Abstraction Layer (DAL) module.

Provides canonical types and protocols for backend-agnostic data access.
"""

from mcp_server.dal.context import get_current_tenant, set_current_tenant
from mcp_server.models.cache.lookup_result import CacheLookupResult
from mcp_server.models.graph.data import GraphData
from mcp_server.models.graph.edge import Edge
from mcp_server.models.graph.node import Node
from mcp_server.models.rag.filters import FilterCriteria

__all__ = [
    # Types
    "Node",
    "Edge",
    "GraphData",
    "FilterCriteria",
    "CacheLookupResult",
    # Context
    "get_current_tenant",
    "set_current_tenant",
]
