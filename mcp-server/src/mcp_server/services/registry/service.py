import logging
from typing import List, Optional

from mcp_server.models import QueryPair
from mcp_server.services.canonicalization import CanonicalizationService
from mcp_server.services.rag import RagEngine

from dal.factory import get_registry_store

logger = logging.getLogger(__name__)


class RegistryService:
    """Unified Registry Service for NLQ-SQL pairs.

    This service orchestrates:
    1. Linguistic canonicalization (matching questions to stable signatures).
    2. Vector embedding (matching questions for semantic similarity).
    3. Multi-role lifecycle management (cache, example, golden).
    """

    @staticmethod
    async def register_pair(
        question: str,
        sql_query: str,
        tenant_id: int,
        roles: List[str],
        status: str = "unverified",
        metadata: Optional[dict] = None,
        performance: Optional[dict] = None,
    ) -> QueryPair:
        """Register or update an NLQ-SQL pair in the unified registry."""
        canonicalizer = CanonicalizationService.get_instance()

        # 1. Generate Canonical Signature
        constraints, fingerprint, signature_key = await canonicalizer.process_query(question)

        # Fallback to raw question hash if canonicalization is disabled or fails
        # This prevents collisions in the registry when SpaCy is unavailable
        if not fingerprint:
            import hashlib

            signature_key = hashlib.sha256(question.lower().strip().encode()).hexdigest()
            fingerprint = f"RAW:{question.lower().strip()}"

        # 2. Generate Embedding
        embedding = await RagEngine.embed_text(question)

        # 3. Create Model
        pair = QueryPair(
            signature_key=signature_key,
            tenant_id=tenant_id,
            fingerprint=fingerprint,
            question=question,
            sql_query=sql_query,
            embedding=embedding,
            roles=roles,
            status=status,
            metadata=metadata or {},
            performance=performance or {},
        )

        # 4. Store
        store = get_registry_store()
        await store.store_pair(pair)

        logger.info(f"✓ Registered QueryPair: {signature_key[:16]}... Roles: {roles}")
        return pair

    @staticmethod
    async def lookup_canonical(question: str, tenant_id: int) -> Optional[QueryPair]:
        """Fetch a specific pair by its canonical signature."""
        canonicalizer = CanonicalizationService.get_instance()
        _, fingerprint, signature_key = await canonicalizer.process_query(question)

        if not fingerprint:
            import hashlib

            signature_key = hashlib.sha256(question.lower().strip().encode()).hexdigest()

        store = get_registry_store()
        return await store.lookup_by_signature(signature_key, tenant_id)

    @staticmethod
    async def lookup_semantic(
        question: str,
        tenant_id: int,
        threshold: float = 0.90,
        limit: int = 5,
        role: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[QueryPair]:
        """Search for semantically similar pairs."""
        embedding = await RagEngine.embed_text(question)
        store = get_registry_store()
        return await store.lookup_semantic_candidates(
            embedding, tenant_id, threshold=threshold, limit=limit, role=role, status=status
        )

    @staticmethod
    async def get_few_shot_examples(
        question: str, tenant_id: int, limit: int = 3
    ) -> List[QueryPair]:
        """Retrieve verified few-shot examples for a question."""
        # We look for 'example' role with 'verified' status
        # Note: Semantic lookup doesn't currently filter by status in the DAL call,
        # but we could add it or filter locally.
        candidates = await RegistryService.lookup_semantic(
            question, tenant_id, threshold=0.70, limit=limit, role="example"
        )

        # Ensure they are verified if we want high trust
        return [c for c in candidates if c.status == "verified"]

    @staticmethod
    async def list_examples(tenant_id: Optional[int] = None, limit: int = 50) -> List[QueryPair]:
        """List all verified few-shot examples."""
        store = get_registry_store()
        return await store.fetch_by_role(
            role="example", status="verified", tenant_id=tenant_id, limit=limit
        )

    @staticmethod
    async def tombstone_pair(signature_key: str, tenant_id: int, reason: str) -> bool:
        """Mark a pair as tombstoned in the registry."""
        store = get_registry_store()
        return await store.tombstone_pair(signature_key, tenant_id, reason)

    @staticmethod
    async def fetch_by_signatures(signature_keys: List[str], tenant_id: int) -> List[QueryPair]:
        """Fetch multiple pairs by their signature keys."""
        store = get_registry_store()
        return await store.fetch_by_signatures(signature_keys, tenant_id)
