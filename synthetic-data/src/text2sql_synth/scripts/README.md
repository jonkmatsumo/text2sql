# Synthetic Data Affordance for Multi-Turn Scripts

This directory contains a catalog of canonical multi-turn conversation scripts used to evaluate Text2SQL model performance in complex, stateful scenarios.

## Dataset Support

Each script in `catalog.yaml` is supported by specific features of the synthetic data generator:

### 1. "Ever Frozen" Logic
- **Table**: `event_account_status_change`
- **Affordance**: The generator produces a full history of status transitions for each account. This includes `active` -> `frozen` -> `active` cycles.
- **Correlation**: Freeze events are strongly correlated with `risk_tier` and `fact_dispute` activity, ensuring realistic clusters of "ever frozen" accounts for analysis.

### 2. Refunds vs Disputes Ambiguity
- **Tables**: `fact_refund`, `fact_dispute`
- **Affordance**: These are distinct but related tables. A transaction can have both a refund and a dispute.
- **Ambiguity**: Scripts test if the model can distinguish between "monetary refunds" and "formal chargeback disputes".

### 3. Gross vs Net Ambiguity
- **Table**: `fact_transaction`
- **Affordance**: Each record contains `gross_amount`, `fee_amount`, and `net_amount`.
- **Ambiguity**: Baseline volume queries are intentionally ambiguous, requiring follow-up turns to clarify whether "volume" refers to gross or net values.

### 4. Geography Ambiguity
- **Tables**: `dim_customer`, `dim_merchant`, `dim_address`
- **Affordance**: Addresses are shared across customers and merchants. 
- **Ambiguity**: Queries for "transactions in California" require the model to resolve whether the location refers to the customer's residence or the merchant's physical location.

### 5. Time Windows and Seasonality
- **Table**: `dim_time`
- **Affordance**: Includes `day_of_week`, `is_weekend`, and `seasonality_factor`.
- **Support**: High-frequency weekly patterns (e.g., weekend spending spikes) are explicitly modeled to support analytical queries about temporal trends.

### 6. Risk Correlations and Rule Decisions
- **Tables**: `fact_transaction`, `event_rule_decision`
- **Affordance**: `fact_transaction.risk_tier` is correlated with the outcome in `event_rule_decision.decision_outcome`.
- **Support**: Allows for investigation into *why* certain risk tiers have higher block rates by joining transactions to their rule engine audit trails.

### 7. Customer History (SCD2)
- **Table**: `dim_customer_scd2`
- **Affordance**: Tracks changes to customer attributes (email, risk tier, segment) over time.
- **Support**: Supports "point-in-time" queries and historical trend analysis of customer metrics.
