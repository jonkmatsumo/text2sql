"""Seed Golden Dataset with initial test cases."""

import asyncio

from dotenv import load_dotenv
from mcp_server.db import Database

load_dotenv()

# Initial Golden test cases
INITIAL_TEST_CASES = [
    {
        "question": "What is the total revenue?",
        "ground_truth_sql": "SELECT SUM(amount) as total_revenue FROM payment;",
        "expected_row_count": 1,
        "category": "aggregation",
        "difficulty": "easy",
        "tenant_id": 1,
    },
    {
        "question": "Show me the top 5 customers by total spending",
        "ground_truth_sql": """
            SELECT c.customer_id, c.first_name, c.last_name, SUM(p.amount) as total_spent
            FROM customer c
            JOIN payment p ON c.customer_id = p.customer_id
            GROUP BY c.customer_id, c.first_name, c.last_name
            ORDER BY total_spent DESC
            LIMIT 5;
        """,
        "expected_row_count": 5,
        "category": "join_aggregation",
        "difficulty": "medium",
        "tenant_id": 1,
    },
    {
        "question": "How many active customers do we have?",
        "ground_truth_sql": """
            SELECT COUNT(*) as active_customers
            FROM customer
            WHERE active = true;
        """,
        "expected_row_count": 1,
        "category": "filter",
        "difficulty": "easy",
        "tenant_id": 1,
    },
    {
        "question": "What are the most rented films?",
        "ground_truth_sql": """
            SELECT f.title, COUNT(r.rental_id) as rental_count
            FROM film f
            JOIN inventory i ON f.film_id = i.film_id
            JOIN rental r ON i.inventory_id = r.inventory_id
            GROUP BY f.film_id, f.title
            ORDER BY rental_count DESC
            LIMIT 10;
        """,
        "expected_row_count": 10,
        "category": "join_aggregation",
        "difficulty": "hard",
        "tenant_id": 1,
    },
    {
        "question": "Calculate monthly revenue for the last 6 months",
        "ground_truth_sql": """
            SELECT
                DATE_TRUNC('month', payment_date) as month,
                SUM(amount) as monthly_revenue
            FROM payment
            WHERE payment_date >= NOW() - INTERVAL '6 months'
            GROUP BY month
            ORDER BY month DESC;
        """,
        "expected_row_count": None,  # Variable based on data
        "category": "time_series",
        "difficulty": "medium",
        "tenant_id": 1,
    },
]


async def seed_golden_dataset():
    """Seed the database with Golden test cases."""
    await Database.init()

    try:
        print("Seeding Golden Dataset...")

        for i, test_case in enumerate(INITIAL_TEST_CASES, 1):
            print(f"\n[{i}/{len(INITIAL_TEST_CASES)}] Processing: {test_case['question']}")

            async with Database.get_connection(test_case.get("tenant_id")) as conn:
                await conn.execute(
                    """
                    INSERT INTO golden_dataset (
                        question, ground_truth_sql, expected_row_count,
                        category, difficulty, tenant_id
                    )
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT DO NOTHING
                """,
                    test_case["question"],
                    test_case["ground_truth_sql"].strip(),
                    test_case["expected_row_count"],
                    test_case["category"],
                    test_case["difficulty"],
                    test_case["tenant_id"],
                )

            print("  ✓ Inserted into database")

        print(f"\n✓ Seeded {len(INITIAL_TEST_CASES)} test cases")

    finally:
        await Database.close()


if __name__ == "__main__":
    asyncio.run(seed_golden_dataset())
