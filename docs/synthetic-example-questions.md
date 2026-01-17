# Synthetic Dataset Example Questions

This document provides example natural language questions for testing the Text2SQL agent against the **synthetic financial dataset** (default mode).

## Schema Overview

| Table Type | Tables |
|------------|--------|
| **Dimensions** | `dim_customer`, `dim_merchant`, `dim_account`, `dim_institution`, `dim_address` |
| **Facts** | `fact_transaction`, `fact_payment`, `fact_refund`, `fact_dispute` |
| **Events** | `event_login`, `event_device`, `event_rule_decision`, `event_account_status_change` |
| **Bridge** | `bridge_customer_address`, `dim_customer_scd2` |

---

## 🟢 Easy (Single Table)

### Count customers
**Question:** "How many customers do we have?"

```sql
SELECT COUNT(*) FROM dim_customer
```

### List active merchants
**Question:** "Show me all active merchants"

```sql
SELECT * FROM dim_merchant WHERE is_active = true
```

### High-risk accounts
**Question:** "List all high-risk accounts"

```sql
SELECT * FROM dim_account WHERE risk_tier = 'high'
```

---

## 🟡 Medium (Joins + Aggregations)

### Top merchants by transaction count
**Question:** "What are the top 5 merchants by transaction count?"

```sql
SELECT m.merchant_name, COUNT(t.transaction_id) as transaction_count
FROM fact_transaction t
JOIN dim_merchant m ON t.merchant_id = m.merchant_id
GROUP BY m.merchant_name
ORDER BY transaction_count DESC
LIMIT 5
```

### Transaction volume by customer segment
**Question:** "Show total transaction volume by customer segment"

```sql
SELECT c.customer_segment, SUM(t.gross_amount) as total_volume
FROM fact_transaction t
JOIN dim_customer c ON t.customer_id = c.customer_id
GROUP BY c.customer_segment
ORDER BY total_volume DESC
```

### Average transaction amount by channel
**Question:** "What's the average transaction amount by channel?"

```sql
SELECT channel, AVG(gross_amount) as avg_amount
FROM fact_transaction
GROUP BY channel
ORDER BY avg_amount DESC
```

---

## 🔴 Difficult (Multi-Join + Complex Logic)

### Refund rate by merchant
**Question:** "What is the refund rate by merchant, showing only merchants with more than 100 transactions?"

```sql
SELECT
    m.merchant_name,
    COUNT(DISTINCT t.transaction_id) as total_transactions,
    COUNT(DISTINCT r.refund_id) as refund_count,
    ROUND(COUNT(DISTINCT r.refund_id)::numeric / COUNT(DISTINCT t.transaction_id) * 100, 2) as refund_rate_pct
FROM fact_transaction t
JOIN dim_merchant m ON t.merchant_id = m.merchant_id
LEFT JOIN fact_refund r ON t.transaction_id = r.transaction_id
GROUP BY m.merchant_name
HAVING COUNT(DISTINCT t.transaction_id) > 100
ORDER BY refund_rate_pct DESC
```

### High-risk customer disputes
**Question:** "Which high-risk customers had disputed transactions resolved in the merchant's favor?"

```sql
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.risk_tier,
    COUNT(d.dispute_id) as dispute_count
FROM dim_customer c
JOIN fact_transaction t ON c.customer_id = t.customer_id
JOIN fact_dispute d ON t.transaction_id = d.transaction_id
WHERE c.risk_tier = 'high'
  AND d.resolution_outcome = 'merchant_won'
GROUP BY c.customer_id, c.first_name, c.last_name, c.risk_tier
ORDER BY dispute_count DESC
```

### Fraud detection analysis
**Question:** "Show customers with more than 3 fraud-flagged transactions in the last 30 days"

```sql
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    COUNT(t.transaction_id) as flagged_count,
    SUM(t.gross_amount) as total_flagged_amount
FROM dim_customer c
JOIN fact_transaction t ON c.customer_id = t.customer_id
WHERE t.is_fraud_flagged = true
  AND t.transaction_ts >= CURRENT_TIMESTAMP - INTERVAL '30 days'
GROUP BY c.customer_id, c.first_name, c.last_name
HAVING COUNT(t.transaction_id) > 3
ORDER BY flagged_count DESC
```

---

## Running These Tests

The synthetic dataset is the **default** when `DATASET_MODE` is not set or set to `synthetic`.

```bash
# Default mode (synthetic)
docker compose -f docker-compose.infra.yml -f docker-compose.app.yml up -d

# Open Streamlit UI
open http://localhost:8501
```

For legacy Pagila testing, see [pagila-example-questions.md](./pagila-example-questions.md) (if available).
