from typing import List, Optional, Protocol, runtime_checkable

from mcp_server.models import QueryPair


@runtime_checkable
class RegistryStore(Protocol):
    """Protocol for unified NLQ-SQL pair registry storage."""

    async def store_pair(self, pair: QueryPair) -> None:
        """Upsert a query pair into the registry."""
        ...

    async def lookup_by_signature(self, signature_key: str, tenant_id: int) -> Optional[QueryPair]:
        """Fetch a specific pair by its canonical signature."""
        ...

    async def lookup_semantic_candidates(
        self,
        embedding: List[float],
        tenant_id: int,
        threshold: float = 0.90,
        limit: int = 5,
        role: Optional[str] = None,
    ) -> List[QueryPair]:
        """Search for semantically similar pairs with optional role filtering."""
        ...

    async def fetch_by_role(
        self,
        role: str,
        status: Optional[str] = None,
        tenant_id: Optional[int] = None,
        limit: int = 100,
    ) -> List[QueryPair]:
        """Fetch pairs by role (e.g., all 'example' pairs)."""
        ...

    async def tombstone_pair(self, signature_key: str, tenant_id: int, reason: str) -> bool:
        """Mark a pair as tombstoned."""
        ...
