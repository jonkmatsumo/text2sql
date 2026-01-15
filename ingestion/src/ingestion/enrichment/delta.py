from typing import Any, Dict, List

from neo4j import Transaction

NODES_NEEDING_ENRICHMENT_QUERY = """
MATCH (n)
WHERE n.source_hash IS NOT NULL
  AND (n.enrichment_source_hash IS NULL OR n.source_hash <> n.enrichment_source_hash)
RETURN n
"""


def get_nodes_needing_enrichment(tx: Transaction) -> List[Dict[str, Any]]:
    """Find nodes that need enrichment (Legacy support for raw sessions).

    A node needs enrichment if:
    1. It has a 'source_hash' property (indicating it is a syncable entity).
    2. Its 'enrichment_source_hash' is NULL (never enriched).
    3. OR its 'source_hash' != 'enrichment_source_hash' (data changed since last enrichment).

    Returns:
        List of node properties (as dictionaries).
    """
    result = tx.run(NODES_NEEDING_ENRICHMENT_QUERY)
    # Convert Neo4j Nodes to dictionaries
    return [dict(record["n"]) for record in result]
