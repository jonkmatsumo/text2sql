"""End-to-end RAG verification test."""

import asyncio

from mcp_server.db import Database
from mcp_server.tools import search_relevant_tables


async def test_rag():
    """Test RAG pipeline end-to-end."""
    await Database.init()

    test_cases = [
        ("Show me actors", ["actor"]),
        ("Customer payment transactions", ["payment", "customer"]),
        ("Movie rental history", ["rental", "film"]),
    ]

    for query, expected_tables in test_cases:
        print(f"\n{'='*60}")
        print(f"Query: {query}")
        print(f"Expected tables: {expected_tables}")
        print(f"{'='*60}")
        result = await search_relevant_tables(query, limit=5)
        print(f"Result:\n{result}\n")

        # Check if expected tables are in result
        found_tables = []
        for expected in expected_tables:
            if expected in result.lower():
                found_tables.append(expected)

        if found_tables:
            print(f"✓ Found expected tables: {found_tables}")
        else:
            print(f"⚠ Warning: Expected tables {expected_tables} not found in results")

    await Database.close()


if __name__ == "__main__":
    asyncio.run(test_rag())
