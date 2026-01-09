"""Data Abstraction Layer (DAL) module.

Provides canonical types and protocols for backend-agnostic data access.
"""

from mcp_server.dal.context import get_current_tenant, set_current_tenant
from mcp_server.dal.types import CacheLookupResult, Edge, FilterCriteria, GraphData, Node

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
