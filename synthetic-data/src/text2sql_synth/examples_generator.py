"""Synthetic few-shot examples generator."""

from typing import Any, Dict

EXAMPLES = [
    {
        "question": "What is the total transaction amount per merchant category?",
        "query": """
SELECT
    m.mcc_description,
    SUM(t.net_amount) as total_amount
FROM fact_transaction t
JOIN dim_merchant m ON t.merchant_id = m.merchant_id
GROUP BY m.mcc_description
ORDER BY total_amount DESC;
        """.strip(),
        "category": "aggregation",
        "difficulty": "medium",
        "filename": "example_01_merchant_category_spend.json",
    },
    {
        "question": "List the top 5 customers by total spending amount.",
        "query": """
SELECT
    c.first_name,
    c.last_name,
    SUM(t.net_amount) as total_spend
FROM fact_transaction t
JOIN dim_customer c ON t.customer_id = c.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_spend DESC
LIMIT 5;
        """.strip(),
        "category": "ranking",
        "difficulty": "medium",
        "filename": "example_02_top_customers.json",
    },
    {
        "question": "How many transactions were flagged as fraud last month?",
        "query": """
SELECT
    COUNT(*) as fraud_count
FROM fact_transaction t
JOIN dim_time d ON t.time_id = d.date_key
WHERE t.is_fraud_flagged = TRUE
  AND d.month_name = 'December'
  AND d.year = 2023; -- Example constraint
        """.strip(),
        "category": "filtering",
        "difficulty": "easy",
        "filename": "example_03_fraud_count.json",
    },
    {
        "question": "Show the daily transaction volume for account 12345.",
        "query": """
SELECT
    d.full_date,
    COUNT(t.transaction_id) as daily_count,
    SUM(t.net_amount) as daily_amount
FROM fact_transaction t
JOIN dim_account a ON t.account_id = a.account_id
JOIN dim_time d ON t.time_id = d.date_key
WHERE a.account_number = '12345'
GROUP BY d.full_date
ORDER BY d.full_date;
        """.strip(),
        "category": "timeseries",
        "difficulty": "hard",
        "filename": "example_04_account_daily_volume.json",
    },
]


def generate_examples() -> Dict[str, Any]:
    """Generate example files content."""
    files = {}
    for ex in EXAMPLES:
        content = {
            "question": ex["question"],
            "query": ex["query"],
            "category": ex["category"],
            "difficulty": ex["difficulty"],
            "tenant_id": 1,  # Default tenant
        }
        files[ex["filename"]] = content
    return files
