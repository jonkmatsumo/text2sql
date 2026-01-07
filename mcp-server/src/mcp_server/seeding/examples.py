"""Seed SQL examples to vector database for few-shot learning."""

import argparse
import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from mcp_server.db import Database
from mcp_server.rag import RagEngine, format_vector_for_postgres
from mcp_server.seeding.loader import load_examples_for_vector_db

load_dotenv()

# Default JSON files if none specified
DEFAULT_PATTERNS = ["database/seed_queries.json"]


async def generate_summary(question: str, sql: str) -> str:
    """Generate a synthetic summary of the SQL logic using LLM.

    This bridges the semantic gap between natural language questions and SQL code.
    """
    llm = ChatOpenAI(
        model=os.getenv("OPENAI_MODEL", "gpt-5.2"),
        temperature=0.3,
    )

    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """You are a SQL expert. Generate a concise natural language summary
        that explains what a SQL query does, focusing on the business logic and intent.
        Do not include SQL syntax details. Write in plain English.""",
            ),
            (
                "user",
                """Question: {question}

SQL Query:
{sql}

Generate a summary explaining what this query does:""",
            ),
        ]
    )

    chain = prompt | llm
    response = await chain.ainvoke({"question": question, "sql": sql.strip()})

    return response.content.strip()


async def seed_examples(
    patterns: list[str],
    base_path: Path,
    dry_run: bool = False,
) -> int:
    """Seed the database with SQL examples and their embeddings.

    Args:
        patterns: File patterns to load queries from.
        base_path: Base path to resolve relative patterns.
        dry_run: If True, only print what would be done.

    Returns:
        Number of examples seeded.
    """
    examples = load_examples_for_vector_db(patterns=patterns, base_path=base_path)

    if not examples:
        print("No examples found to seed.")
        return 0

    if not dry_run:
        await Database.init()

    try:
        print(f"Seeding {len(examples)} examples...")

        for i, example in enumerate(examples, 1):
            print(f"\n[{i}/{len(examples)}] Processing: {example['question']}")

            if dry_run:
                print("  (dry run - skipping)")
                continue

            # Generate synthetic summary
            summary = await generate_summary(example["question"], example["query"])
            print(f"  Summary: {summary[:80]}...")

            # Generate embedding from summary
            embedding = RagEngine.embed_text(summary)
            pg_vector = format_vector_for_postgres(embedding)

            # Insert into database
            async with Database.get_connection() as conn:
                await conn.execute(
                    """
                    INSERT INTO sql_examples (question, sql_query, summary, embedding)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT DO NOTHING
                """,
                    example["question"],
                    example["query"].strip(),
                    summary,
                    pg_vector,
                )

            print("  ✓ Inserted into database")

        print(f"\n✓ Seeded {len(examples)} examples")
        return len(examples)

    finally:
        if not dry_run:
            await Database.close()


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Seed SQL examples to vector DB")
    parser.add_argument(
        "--files",
        nargs="+",
        default=DEFAULT_PATTERNS,
        help="JSON file patterns to load (default: database/seed_queries.json)",
    )
    parser.add_argument(
        "--base-path",
        type=Path,
        default=Path.cwd(),
        help="Base path for resolving relative file patterns",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without making changes",
    )

    args = parser.parse_args()
    asyncio.run(seed_examples(args.files, args.base_path, args.dry_run))


if __name__ == "__main__":
    main()
