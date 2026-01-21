import asyncio
import logging

from common.interfaces import GraphStore

from .agent import EnrichmentAgent
from .config import PipelineConfig
from .hashing import generate_canonical_hash
from .loader import replay_wal
from .wal import WALManager

logger = logging.getLogger(__name__)


class EnrichmentPipeline:
    """Orchestrates the semantic enrichment process (Robust & Idempotent)."""

    def __init__(self, store: GraphStore, dry_run: bool = False):
        """Initialize the pipeline."""
        self.config = PipelineConfig(dry_run=dry_run)
        if store is None:
            raise ValueError("store is required")
        self.store = store
        self.semaphore = asyncio.Semaphore(5)  # Enforce concurrency limit

    def close(self):
        """Close the database driver."""
        self.store.close()

    async def run(self):
        """Execute the enrichment pipeline."""
        logger.info("Starting enrichment pipeline...")

        # Initialize persistence manager
        wal_manager = WALManager()

        # Phase 1: Recovery (The Fix)
        # Apply any pending work from previous runs BEFORE determining what's stale.
        # This prevents double-billing by syncing the DB with the WAL state.
        self._commit_wal_to_db(wal_manager.file_path, "Recovery Phase")

        # Phase 2: Delta Detection
        nodes_to_enrich = []
        # get_nodes_needing_enrichment is a Cypher query helper
        # Since we refactored to GraphStore, we can use run_query
        from .delta import NODES_NEEDING_ENRICHMENT_QUERY

        results = self.store.run_query(NODES_NEEDING_ENRICHMENT_QUERY)
        # Note: run_query returns list of dicts.
        # Delta helper used to return [dict(record["n"]) for record in result]
        # In run_query, each dict is record, which has keys like "n".
        nodes_to_enrich = [dict(record["n"]) for record in results]

        if not nodes_to_enrich:
            logger.info("No nodes need enrichment.")
            return

        logger.info(f"Found {len(nodes_to_enrich)} nodes to enrich.")

        # Phase 3: Async Generation Loop (Throttled)
        agent = EnrichmentAgent()

        # We define a helper task for each node
        tasks = [self._process_node_safely(agent, wal_manager, node) for node in nodes_to_enrich]

        # Execute concurrently
        await asyncio.gather(*tasks)

        # Phase 4: Final Commit
        # Apply the newly generated work to the DB.
        self._commit_wal_to_db(wal_manager.file_path, "Final Commit Phase")

    async def _process_node_safely(
        self, agent: EnrichmentAgent, wal_manager: WALManager, node: dict
    ):
        """
        Process a single node safely with error handling and throttling.

        1. Acquire semaphore (throttle).
        2. Generate description (LLM).
        3. Write to WAL (persist).
        4. Handle errors (poison pill protection).
        """
        async with self.semaphore:
            try:
                description = await agent.generate_description(node)
                if description:
                    # We use 'elementId' if available (Neo4j 5+), else 'id' (int)
                    # This must match what is used in Delta Detection query logic
                    node_id = node.get("elementId") or str(node.get("id"))

                    # Create a clean dict for hashing
                    hash_data = {k: v for k, v in node.items() if k != "enrichment_source_hash"}
                    new_hash = generate_canonical_hash(hash_data)

                    # Persist immediately to WAL
                    wal_manager.append_entry(node_id, description, new_hash)
            except Exception as e:
                logger.error(f"Failed to process node {node.get('id')} ({node.get('name')}): {e}")
                # We return None/suppress error so other tasks continue

    def _commit_wal_to_db(self, wal_path: str, phase_name: str):
        """Read the WAL and commit entries to the database."""
        count = 0
        logger.info(f"[{phase_name}] Syncing WAL to Database...")
        for entry in replay_wal(wal_path):
            try:
                self._commit_entry(entry)
                count += 1
            except Exception as e:
                logger.error(f"Error commiting entry {entry.get('node_id')}: {e}")

        logger.info(f"[{phase_name}] Committed {count} entries.")

    def _commit_entry(self, entry: dict):
        """Execute Cypher to update the node."""
        node_id_str = entry["node_id"]

        # Attempt to handle integer ID for Memgraph legacy support
        try:
            # If it looks like an int, treat as internal ID
            node_id = int(node_id_str)
            query = """
             MATCH (n) WHERE id(n) = $node_id
             SET n.description = $description,
                 n.enrichment_source_hash = $new_hash
             """
        except ValueError:
            # Assume elementId (string)
            node_id = node_id_str
            query = """
             MATCH (n) WHERE elementId(n) = $node_id
             SET n.description = $description,
                 n.enrichment_source_hash = $new_hash
             """

        self.store.run_query(
            query,
            parameters={
                "node_id": node_id,
                "description": entry["description"],
                "new_hash": entry["new_hash"],
            },
        )
