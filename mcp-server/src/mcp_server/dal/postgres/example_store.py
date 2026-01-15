import json
from contextlib import asynccontextmanager
from typing import List, Optional

from mcp_server.config.database import Database
from mcp_server.models import Example

from common.interfaces.example_store import ExampleStore


class PostgresExampleStore(ExampleStore):
    """Postgres implementation of ExampleStore."""

    @staticmethod
    @asynccontextmanager
    async def _get_connection(tenant_id: Optional[int] = None):
        """Get connection from control-plane pool if enabled, else main pool."""
        from mcp_server.config.control_plane import ControlPlaneDatabase

        if ControlPlaneDatabase.is_enabled():
            async with ControlPlaneDatabase.get_connection(tenant_id) as conn:
                yield conn
        else:
            async with Database.get_connection(tenant_id) as conn:
                yield conn

    async def fetch_all_examples(self) -> List[Example]:
        """Fetch all examples from sql_examples table.

        Returns:
            List of canonical Example objects.
        """
        query = """
            SELECT id, question, sql_query, embedding
            FROM sql_examples
            WHERE embedding IS NOT NULL
        """

        async with self._get_connection() as conn:
            rows = await conn.fetch(query)

        examples = []
        for row in rows:
            embedding_val = row["embedding"]
            if isinstance(embedding_val, str):
                vector = json.loads(embedding_val)
            else:
                vector = list(embedding_val)

            examples.append(
                Example(
                    id=row["id"],
                    question=row["question"],
                    sql_query=row["sql_query"],
                    embedding=vector,
                )
            )

        return examples
