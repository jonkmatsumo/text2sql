"""Script to seed the database with Golden SQL examples and synthetic summaries."""

import asyncio
import os

from dotenv import load_dotenv
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from mcp_server.db import Database
from mcp_server.rag import RagEngine, format_vector_for_postgres

load_dotenv()


# Initial Golden SQL examples (question, SQL pairs)
INITIAL_EXAMPLES = [
    {
        "question": "What is the total revenue?",
        "sql": "SELECT SUM(amount) as total_revenue FROM payment;",
    },
    {
        "question": "Show me the top 5 customers by total spending",
        "sql": """
            SELECT c.customer_id, c.first_name, c.last_name, SUM(p.amount) as total_spent
            FROM customer c
            JOIN payment p ON c.customer_id = p.customer_id
            GROUP BY c.customer_id, c.first_name, c.last_name
            ORDER BY total_spent DESC
            LIMIT 5;
        """,
    },
    {
        "question": "How many active customers do we have?",
        "sql": """
            SELECT COUNT(*) as active_customers
            FROM customer
            WHERE active = true;
        """,
    },
    {
        "question": "What are the most rented films?",
        "sql": """
            SELECT f.title, COUNT(r.rental_id) as rental_count
            FROM film f
            JOIN inventory i ON f.film_id = i.film_id
            JOIN rental r ON i.inventory_id = r.inventory_id
            GROUP BY f.film_id, f.title
            ORDER BY rental_count DESC
            LIMIT 10;
        """,
    },
    {
        "question": "Calculate monthly revenue for the last 6 months",
        "sql": """
            SELECT
                DATE_TRUNC('month', payment_date) as month,
                SUM(amount) as monthly_revenue
            FROM payment
            WHERE payment_date >= NOW() - INTERVAL '6 months'
            GROUP BY month
            ORDER BY month DESC;
        """,
    },
]


async def generate_summary(question: str, sql: str) -> str:
    """
    Generate a synthetic summary of the SQL logic using LLM.

    This bridges the semantic gap between natural language questions and SQL code.
    """
    llm = ChatOpenAI(
        model=os.getenv("OPENAI_MODEL", "gpt-4o"),
        temperature=0.3,  # Lower temperature for more consistent summaries
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


async def seed_examples():
    """Seed the database with Golden SQL examples and their embeddings."""
    await Database.init()

    try:
        print("Generating synthetic summaries and embeddings...")

        for i, example in enumerate(INITIAL_EXAMPLES, 1):
            print(f"\n[{i}/{len(INITIAL_EXAMPLES)}] Processing: {example['question']}")

            # Generate synthetic summary
            summary = await generate_summary(example["question"], example["sql"])
            print(f"  Summary: {summary}")

            # Generate embedding from summary (not from SQL)
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
                    example["sql"].strip(),
                    summary,
                    pg_vector,
                )

            print("  ✓ Inserted into database")

        print(f"\n✓ Seeded {len(INITIAL_EXAMPLES)} examples")

    finally:
        await Database.close()


if __name__ == "__main__":
    asyncio.run(seed_examples())
