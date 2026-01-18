"""Query grounding utilities for pre-retrieval canonicalization.

This module provides a lightweight mechanism to ground user queries
with schema hints before passing them to semantic search.
"""

import logging
from typing import Dict, List, Tuple

logger = logging.getLogger(__name__)


def extract_schema_hints(query: str) -> Tuple[str, List[Dict[str, str]]]:
    """Extract schema-relevant entities and their canonical mappings from a query.

    Attempts to use the CanonicalizationService if available (SpaCy-based).
    Falls back gracefully if SpaCy is not available or disabled.

    Args:
        query: The user's natural language query.

    Returns:
        Tuple of (grounded_query, mappings) where:
        - grounded_query: The original query with optional schema hints appended
        - mappings: List of {mention: ..., canonical_id: ...} for observability
    """
    mappings: List[Dict[str, str]] = []

    try:
        from mcp_server.services.canonicalization import CanonicalizationService

        service = CanonicalizationService.get_instance()
        if not service.is_available():
            logger.debug("CanonicalizationService not available, using raw query")
            return query, []

        # Get the SpaCy NLP pipeline
        nlp = service.nlp
        if not nlp:
            return query, []

        doc = nlp(query)

        # Extract entities that have canonical IDs (from EntityRuler)
        for ent in doc.ents:
            if ent.ent_id_:
                mappings.append(
                    {
                        "mention": ent.text,
                        "canonical_id": ent.ent_id_,
                        "label": ent.label_,
                    }
                )

    except ImportError:
        logger.debug("CanonicalizationService not importable, using raw query")
        return query, []
    except Exception as e:
        logger.warning(f"Error during query grounding: {e}")
        return query, []

    if not mappings:
        return query, []

    # Build grounded query with schema hints
    hints = ", ".join([f"{m['mention']}≈{m['canonical_id']}" for m in mappings])
    grounded_query = f"{query}\n\nSchema hints: {hints}"

    logger.info(f"Grounded query with {len(mappings)} schema hints")
    return grounded_query, mappings


def ground_query_for_retrieval(query: str) -> str:
    """Ground a query for schema retrieval.

    This is the main entry point for pre-retrieval canonicalization.
    Returns a query string that may include schema hints if synonyms are detected.

    Args:
        query: The user's natural language query.

    Returns:
        Grounded query string ready for get_semantic_subgraph.
    """
    grounded, _ = extract_schema_hints(query)
    return grounded
