"""Canonical alias service for schema enrichment.

Provides lookups from nlp_patterns table to enrich schema nodes
with their known synonyms/aliases.
"""

import logging
from typing import Dict, List

from mcp_server.config.database import Database

logger = logging.getLogger(__name__)


class CanonicalAliasService:
    """Service for looking up canonical aliases from nlp_patterns."""

    # Cache for aliases, keyed by label (TABLE, COLUMN)
    _cache: Dict[str, Dict[str, List[str]]] = {}
    _loaded: bool = False

    @classmethod
    async def load_aliases(cls) -> None:
        """Load all aliases from nlp_patterns into memory.

        Groups aliases by label and canonical_id.
        """
        if cls._loaded:
            return

        try:
            async with Database.get_connection() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, label, pattern
                    FROM nlp_patterns
                    WHERE label IN ('TABLE', 'COLUMN')
                    """
                )

                # Reset cache
                cls._cache = {}

                for row in rows:
                    canonical_id = row["id"]
                    label = row["label"]
                    pattern = row["pattern"]

                    if label not in cls._cache:
                        cls._cache[label] = {}

                    if canonical_id not in cls._cache[label]:
                        cls._cache[label][canonical_id] = []

                    # Don't include the canonical name itself as an alias
                    if pattern.lower() != canonical_id.lower():
                        cls._cache[label][canonical_id].append(pattern)

                cls._loaded = True
                total = sum(
                    len(aliases) for by_id in cls._cache.values() for aliases in by_id.values()
                )
                logger.info(f"Loaded {total} canonical aliases for schema enrichment")

        except Exception as e:
            logger.warning(f"Failed to load canonical aliases: {e}")
            cls._cache = {}

    @classmethod
    async def get_aliases_for_table(cls, table_name: str) -> List[str]:
        """Get known aliases/synonyms for a table.

        Args:
            table_name: The canonical table name (e.g., "users").

        Returns:
            List of known aliases (e.g., ["customers", "buyers"]).
        """
        if not cls._loaded:
            await cls.load_aliases()

        return cls._cache.get("TABLE", {}).get(table_name, [])

    @classmethod
    async def get_aliases_for_column(cls, column_id: str) -> List[str]:
        """Get known aliases/synonyms for a column.

        Args:
            column_id: The canonical column ID (e.g., "users.email").

        Returns:
            List of known aliases.
        """
        if not cls._loaded:
            await cls.load_aliases()

        return cls._cache.get("COLUMN", {}).get(column_id, [])

    @classmethod
    def invalidate_cache(cls) -> None:
        """Invalidate the alias cache (e.g., after pattern regeneration)."""
        cls._cache = {}
        cls._loaded = False
