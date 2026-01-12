"""Table schema definitions and generation order for synthetic data."""

from __future__ import annotations

# The order in which tables must be generated to satisfy foreign key dependencies.
GENERATION_ORDER = [
    "dim_time",
    "dim_institution",
    "dim_address",
    "dim_customer",
    "dim_merchant",
    "dim_account",
    "bridge_customer_address",
    "dim_counterparty",
    "dim_customer_scd2",
    "fact_transaction",
    "fact_payment",
    "fact_refund",
    "fact_dispute",
]

# Explicit dependencies for each table.
DEPENDENCIES = {
    "dim_time": [],
    "dim_institution": [],
    "dim_address": [],
    "dim_customer": ["dim_address"],
    "dim_merchant": ["dim_address"],
    "dim_account": ["dim_customer", "dim_institution"],
    "bridge_customer_address": ["dim_customer", "dim_address"],
    "dim_counterparty": ["dim_merchant"],
    "dim_customer_scd2": ["dim_customer"],
    "fact_transaction": ["dim_time", "dim_customer", "dim_account", "dim_merchant", "dim_counterparty", "dim_institution"],
    "fact_payment": ["fact_transaction"],
    "fact_refund": ["fact_transaction"],
    "fact_dispute": ["fact_transaction"],
}

# Expected columns for each table to enable immediate validation after generation.
EXPECTED_COLUMNS = {
    "dim_time": [
        "date_key", "full_date", "day_of_week", "day_name", "day_of_month", 
        "day_of_year", "week_of_year", "month", "month_name", "quarter", 
        "year", "is_weekend", "is_holiday", "is_month_start", "is_month_end", 
        "is_quarter_start", "is_quarter_end", "seasonality_factor"
    ],
    "dim_institution": [
        "institution_id", "institution_name", "institution_type", "country_code", "is_active", "routing_number", "swift_code"
    ],
    "dim_address": [
        "address_id", "address_line1", "address_line2", "city", 
        "state_code", "state_name", "postal_code", "country_code", "latitude", "longitude", "address_type"
    ],
    "dim_customer": [
        "customer_id", "first_name", "last_name", "email", "phone", 
        "date_of_birth", "customer_since", "risk_tier", "customer_segment", "is_active", "activity_score", "primary_address_id"
    ],
    "dim_merchant": [
        "merchant_id", "merchant_name", "mcc_code", "mcc_description", "address_id", 
        "acquirer_id", "risk_tier", "is_active", "popularity_score", "avg_transaction_amount", "established_date"
    ],
    "dim_account": [
        "account_id", "customer_id", "institution_id", "account_type", 
        "account_number", "account_status", "currency", "opened_date", "closed_date", "credit_limit", "risk_tier"
    ],
    "bridge_customer_address": [
        "bridge_id", "customer_id", "address_id", "address_type", "is_current", "effective_from", "effective_to"
    ],
    "dim_counterparty": [
        "counterparty_id", "counterparty_type", "counterparty_name", "merchant_id", "external_id", "risk_tier", "is_verified", "country_code"
    ],
    "dim_customer_scd2": [
        "scd_id", "customer_id", "first_name", "last_name", "email", "risk_tier", "customer_segment", "is_active", "effective_from", "effective_to", "is_current", "version_number"
    ],
    "fact_transaction": [
        "transaction_id", "account_id", "customer_id", "merchant_id", 
        "counterparty_id", "institution_id", "time_id", "transaction_ts", 
        "gross_amount", "fee_amount", "net_amount", "currency", "channel", 
        "status", "risk_tier", "device_id", "is_emulator", "is_fraud_flagged"
    ],
    "fact_payment": [
        "payment_id", "transaction_id", "payment_method", "card_network", "card_last_four", "auth_code", "settlement_status", "settlement_ts", "ach_return_code", "ach_return_reason", "processing_fee", "interchange_fee"
    ],
    "fact_refund": [
        "refund_id", "transaction_id", "refund_amount", "refund_reason", "refund_status", "refund_requested_ts", "refund_processed_ts", "is_partial", "refund_method", "processing_fee_refunded"
    ],
    "fact_dispute": [
        "dispute_id", "transaction_id", "dispute_reason", "dispute_status", "dispute_amount", "dispute_opened_ts", "dispute_resolved_ts", "resolution_outcome", "is_chargeback", "merchant_responded", "evidence_submitted", "days_to_resolution"
    ],
}
