"""Table generators for synthetic data generation.

Each module in this package exposes a generate() function that takes a
GenerationContext and SynthConfig and returns a pandas DataFrame.
"""

# Dimension generators
from text2sql_synth.generators.bridge_customer_address import (
    generate as generate_bridge_customer_address,
)
from text2sql_synth.generators.dim_account import generate as generate_dim_account
from text2sql_synth.generators.dim_address import generate as generate_dim_address
from text2sql_synth.generators.dim_counterparty import (
    generate as generate_dim_counterparty,
)
from text2sql_synth.generators.dim_customer import generate as generate_dim_customer
from text2sql_synth.generators.dim_customer_scd2 import (
    generate as generate_dim_customer_scd2,
)
from text2sql_synth.generators.dim_institution import (
    generate as generate_dim_institution,
)
from text2sql_synth.generators.dim_merchant import generate as generate_dim_merchant
from text2sql_synth.generators.dim_time import generate as generate_dim_time

# Event generators
from text2sql_synth.generators.event_account_balance_daily import (
    generate as generate_event_account_balance_daily,
)
from text2sql_synth.generators.event_account_status_change import (
    generate as generate_event_account_status_change,
)
from text2sql_synth.generators.event_device import generate as generate_event_device
from text2sql_synth.generators.event_login import generate as generate_event_login
from text2sql_synth.generators.event_rule_decision import (
    generate as generate_event_rule_decision,
)

# Fact generators
from text2sql_synth.generators.fact_dispute import generate as generate_fact_dispute
from text2sql_synth.generators.fact_payment import generate as generate_fact_payment
from text2sql_synth.generators.fact_refund import generate as generate_fact_refund
from text2sql_synth.generators.fact_transaction import (
    generate as generate_fact_transaction,
)

__all__ = [
    # Dimensions
    "generate_dim_time",
    "generate_dim_institution",
    "generate_dim_address",
    "generate_dim_customer",
    "generate_dim_account",
    "generate_dim_merchant",
    "generate_bridge_customer_address",
    "generate_dim_counterparty",
    "generate_dim_customer_scd2",
    # Facts
    "generate_fact_transaction",
    "generate_fact_payment",
    "generate_fact_refund",
    "generate_fact_dispute",
    # Events
    "generate_event_login",
    "generate_event_device",
    "generate_event_account_status_change",
    "generate_event_rule_decision",
    "generate_event_account_balance_daily",
]
