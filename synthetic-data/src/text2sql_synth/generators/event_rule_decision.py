"""Generator for event_rule_decision event table.

Rule engine decisions for transactions and logins.
"""

from __future__ import annotations

from datetime import timedelta

import numpy as np
import pandas as pd

from text2sql_synth.config import SynthConfig
from text2sql_synth.context import GenerationContext

TABLE_NAME = "event_rule_decision"

# Decision outcomes
DECISION_OUTCOMES = ["allow", "block", "review"]

# Rule names
TRANSACTION_RULES = [
    "velocity_24h",
    "amount_threshold",
    "merchant_risk",
    "device_reputation",
    "geo_anomaly",
    "emulator_check",
    "card_not_present",
    "high_risk_mcc",
    "new_device",
    "unusual_time",
]

LOGIN_RULES = [
    "failed_attempts",
    "device_binding",
    "geo_velocity",
    "known_bad_ip",
    "browser_fingerprint",
    "session_hijack",
    "credential_stuffing",
    "bot_detection",
]

# Risk tier block rate multipliers
RISK_BLOCK_MULTIPLIERS = {
    "low": 0.3,
    "medium": 1.0,
    "high": 2.5,
    "critical": 5.0,
}


def generate(ctx: GenerationContext, cfg: SynthConfig) -> pd.DataFrame:
    """Generate the event_rule_decision event table.

    Creates rule decision records with:
    - decision_id: Unique identifier
    - event_type: transaction or login
    - event_id: FK to fact_transaction or event_login
    - account_id: FK to dim_account
    - customer_id: FK to dim_customer
    - decision_ts: When decision was made
    - decision_outcome: allow, block, review
    - rules_triggered: List of rule names that triggered
    - rule_scores: Aggregate risk score from rules
    - model_score: ML model score (0-1000)
    - decision_reason: Primary reason for decision
    - review_queue: Queue assignment if review (nullable)
    - review_completed: Whether review was completed
    - final_outcome: Final outcome after review (nullable)

    Block rates are correlated with risk tier.
    Transactions and logins both get rule decisions.

    Args:
        ctx: Generation context with RNG.
        cfg: Configuration.

    Returns:
        DataFrame with rule decision data.
    """
    rng = ctx.rng_for(TABLE_NAME)

    # Get transaction data
    transaction_df = ctx.get_table("fact_transaction")
    if transaction_df is None:
        transaction_df = pd.DataFrame()

    # Get login data
    login_df = ctx.get_table("event_login")
    if login_df is None:
        login_df = pd.DataFrame()

    # Get account risk tiers
    account_df = ctx.get_table("dim_account")
    account_risk = dict(zip(account_df["account_id"], account_df["risk_tier"]))

    rows = []

    # Process transactions
    for _, txn_row in transaction_df.iterrows():
        decision_id = ctx.stable_id("rule")
        risk_tier = txn_row["risk_tier"]
        risk_multiplier = RISK_BLOCK_MULTIPLIERS.get(risk_tier, 1.0)

        # Decision timing (slightly before transaction completes)
        decision_ts = txn_row["transaction_ts"] - timedelta(seconds=int(rng.integers(1, 5)))

        # Determine outcome based on transaction status and risk
        if txn_row["status"] == "declined":
            # Declined transactions were blocked
            decision_outcome = "block"
            # Select rules that would have triggered
            num_rules = int(rng.integers(1, 4))
            rules_triggered = list(rng.choice(TRANSACTION_RULES, size=num_rules, replace=False))
            rule_scores = int(rng.integers(70, 100))
            model_score = int(rng.integers(700, 1000))
            decision_reason = rules_triggered[0]
        elif txn_row["is_fraud_flagged"] or txn_row["is_emulator"]:
            # Flagged but allowed - went to review
            if rng.random() < 0.6:
                decision_outcome = "review"
                num_rules = int(rng.integers(1, 3))
                rules_triggered = list(rng.choice(TRANSACTION_RULES, size=num_rules, replace=False))
                rule_scores = int(rng.integers(40, 70))
                model_score = int(rng.integers(400, 700))
                decision_reason = "flagged_for_review"
            else:
                decision_outcome = "allow"
                rules_triggered = []
                rule_scores = int(rng.integers(10, 40))
                model_score = int(rng.integers(100, 400))
                decision_reason = "passed_checks"
        else:
            # Normal transactions
            base_block_rate = 0.01 * risk_multiplier
            base_review_rate = 0.03 * risk_multiplier

            roll = rng.random()
            if roll < base_block_rate:
                decision_outcome = "block"
                num_rules = int(rng.integers(1, 3))
                rules_triggered = list(rng.choice(TRANSACTION_RULES, size=num_rules, replace=False))
                rule_scores = int(rng.integers(70, 95))
                model_score = int(rng.integers(700, 950))
                decision_reason = rules_triggered[0]
            elif roll < base_block_rate + base_review_rate:
                decision_outcome = "review"
                num_rules = int(rng.integers(1, 2))
                rules_triggered = list(rng.choice(TRANSACTION_RULES, size=num_rules, replace=False))
                rule_scores = int(rng.integers(40, 70))
                model_score = int(rng.integers(400, 700))
                decision_reason = "risk_threshold"
            else:
                decision_outcome = "allow"
                rules_triggered = []
                rule_scores = int(rng.integers(0, 30))
                model_score = int(rng.integers(0, 300))
                decision_reason = "passed_checks"

        # Review queue assignment
        review_queue = None
        review_completed = False
        final_outcome = None

        if decision_outcome == "review":
            review_queue = ctx.sample_categorical(
                rng,
                ["fraud_team", "risk_team", "compliance", "general"],
                weights=[0.4, 0.3, 0.2, 0.1],
            )
            review_completed = rng.random() < 0.8
            if review_completed:
                final_outcome = ctx.sample_categorical(
                    rng, ["approved", "rejected"], weights=[0.7, 0.3]
                )

        row = {
            "decision_id": decision_id,
            "event_type": "transaction",
            "event_id": txn_row["transaction_id"],
            "account_id": txn_row["account_id"],
            "customer_id": txn_row["customer_id"],
            "decision_ts": decision_ts,
            "decision_outcome": decision_outcome,
            "rules_triggered": ",".join(rules_triggered) if rules_triggered else None,
            "rule_scores": rule_scores,
            "model_score": model_score,
            "decision_reason": decision_reason,
            "review_queue": review_queue,
            "review_completed": review_completed,
            "final_outcome": final_outcome,
        }
        rows.append(row)

    # Process logins (sample to avoid huge table)
    if len(login_df) > 0:
        # Process all logins (they're already sized appropriately)
        for _, login_row in login_df.iterrows():
            # Only create decisions for ~30% of logins
            if rng.random() > 0.3:
                continue

            decision_id = ctx.stable_id("rule")
            account_id = login_row["account_id"]
            risk_tier = account_risk.get(account_id, "low") if account_id else "low"
            risk_multiplier = RISK_BLOCK_MULTIPLIERS.get(risk_tier, 1.0)

            decision_ts = login_row["login_ts"] - timedelta(seconds=int(rng.integers(0, 2)))

            # Determine outcome based on login result
            if login_row["login_outcome"] != "success":
                # Failed logins may have been blocked
                if rng.random() < 0.3:
                    decision_outcome = "block"
                    num_rules = int(rng.integers(1, 3))
                    rules_triggered = list(rng.choice(LOGIN_RULES, size=num_rules, replace=False))
                    rule_scores = int(rng.integers(60, 90))
                    model_score = int(rng.integers(600, 900))
                    decision_reason = rules_triggered[0]
                else:
                    decision_outcome = "allow"
                    rules_triggered = []
                    rule_scores = int(rng.integers(10, 50))
                    model_score = int(rng.integers(100, 500))
                    decision_reason = "authentication_handled"
            else:
                base_block_rate = 0.005 * risk_multiplier
                base_review_rate = 0.02 * risk_multiplier

                roll = rng.random()
                if roll < base_block_rate:
                    decision_outcome = "block"
                    num_rules = int(rng.integers(1, 2))
                    rules_triggered = list(rng.choice(LOGIN_RULES, size=num_rules, replace=False))
                    rule_scores = int(rng.integers(70, 95))
                    model_score = int(rng.integers(700, 950))
                    decision_reason = rules_triggered[0]
                elif roll < base_block_rate + base_review_rate:
                    decision_outcome = "review"
                    num_rules = int(rng.integers(1, 2))
                    rules_triggered = list(rng.choice(LOGIN_RULES, size=num_rules, replace=False))
                    rule_scores = int(rng.integers(40, 70))
                    model_score = int(rng.integers(400, 700))
                    decision_reason = "risk_threshold"
                else:
                    decision_outcome = "allow"
                    rules_triggered = []
                    rule_scores = int(rng.integers(0, 25))
                    model_score = int(rng.integers(0, 250))
                    decision_reason = "passed_checks"

            # Review handling
            review_queue = None
            review_completed = False
            final_outcome = None

            if decision_outcome == "review":
                review_queue = ctx.sample_categorical(
                    rng,
                    ["security_team", "fraud_team", "general"],
                    weights=[0.5, 0.3, 0.2],
                )
                review_completed = rng.random() < 0.75
                if review_completed:
                    final_outcome = ctx.sample_categorical(
                        rng, ["approved", "rejected"], weights=[0.8, 0.2]
                    )

            row = {
                "decision_id": decision_id,
                "event_type": "login",
                "event_id": login_row["login_id"],
                "account_id": account_id,
                "customer_id": login_row["customer_id"],
                "decision_ts": decision_ts,
                "decision_outcome": decision_outcome,
                "rules_triggered": ",".join(rules_triggered) if rules_triggered else None,
                "rule_scores": rule_scores,
                "model_score": model_score,
                "decision_reason": decision_reason,
                "review_queue": review_queue,
                "review_completed": review_completed,
                "final_outcome": final_outcome,
            }
            rows.append(row)

    df = pd.DataFrame(rows)
    ctx.register_table(TABLE_NAME, df)

    return df
