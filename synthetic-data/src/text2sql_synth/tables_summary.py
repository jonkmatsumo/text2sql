"""Table summaries for synthetic schema."""

from typing import Any, Dict, List

from text2sql_synth.schema import GENERATION_ORDER

TABLE_DESCRIPTIONS: Dict[str, str] = {
    "dim_time": (
        "Time dimension containing date attributes for analysis "
        "(day, month, quarter, year, holidays)."
    ),
    "dim_institution": "Financial institutions (banks) where accounts are held.",
    "dim_address": "Address dimension for customers and merchants.",
    "dim_customer": ("Customer dimension containing demographic and risk profile information."),
    "dim_merchant": (
        "Merchant dimension containing store details, category (MCC), and risk scores."
    ),
    "dim_account": (
        "Account dimension linking customers to institutions, including account limits and status."
    ),
    "bridge_customer_address": (
        "Bridge table linking customers to addresses (many-to-many history)."
    ),
    "dim_counterparty": "Counterparties involved in transactions (recipients).",
    "dim_customer_scd2": ("Slowly Changing Dimension Type 2 for Customer history tracking."),
    "fact_transaction": (
        "Central fact table recording all financial transactions and their status."
    ),
    "fact_payment": (
        "Payment details associated with transactions (card network, fees, auth codes)."
    ),
    "fact_refund": "Refund events linked to original transactions.",
    "fact_dispute": "Dispute/Chargeback events raised against transactions.",
    "event_login": "Customer login events for security analysis (IP, device, outcome).",
    "event_device": "Device registry tracking devices used by customers.",
    "event_account_status_change": "History of account status changes (e.g., active to suspended).",
    "event_rule_decision": "Fraud rule engine decisions and scoring outcomes for events.",
    "event_account_balance_daily": "Daily snapshot of account balances and turnover.",
}


def generate_tables_summary() -> List[Dict[str, Any]]:
    """Generate list of table summaries for seeding."""
    summaries = []
    for table in GENERATION_ORDER:
        description = TABLE_DESCRIPTIONS.get(table, f"Table {table} in synthetic schema.")
        summaries.append({"table_name": table, "summary": description})
    return summaries
