"""Validation suite for synthetic data."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    """Container for validation results."""
    is_valid: bool
    metrics: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

FK_MAPPINGS = {
    "dim_customer": [("primary_address_id", "dim_address", "address_id")],
    "dim_merchant": [("address_id", "dim_address", "address_id")],
    "dim_account": [
        ("customer_id", "dim_customer", "customer_id"),
        ("institution_id", "dim_institution", "institution_id"),
    ],
    "bridge_customer_address": [
        ("customer_id", "dim_customer", "customer_id"),
        ("address_id", "dim_address", "address_id"),
    ],
    "dim_counterparty": [("merchant_id", "dim_merchant", "merchant_id")],
    "dim_customer_scd2": [("customer_id", "dim_customer", "customer_id")],
    "fact_transaction": [
        ("account_id", "dim_account", "account_id"),
        ("customer_id", "dim_customer", "customer_id"),
        ("merchant_id", "dim_merchant", "merchant_id"),
        ("counterparty_id", "dim_counterparty", "counterparty_id"),
        ("institution_id", "dim_institution", "institution_id"),
        ("time_id", "dim_time", "date_key"),
    ],
    "fact_payment": [("transaction_id", "fact_transaction", "transaction_id")],
    "fact_refund": [("transaction_id", "fact_transaction", "transaction_id")],
    "fact_dispute": [("transaction_id", "fact_transaction", "transaction_id")],
}

def validate_manifest(manifest_path: str | Path) -> ValidationResult:
    """Validate a generated dataset using its manifest.

    Args:
        manifest_path: Path to the manifest.json file.

    Returns:
        ValidationResult object.
    """
    manifest_path = Path(manifest_path)
    if not manifest_path.exists():
        return ValidationResult(is_valid=False, errors=[f"Manifest not found: {manifest_path}"])

    try:
        with open(manifest_path, "r") as f:
            manifest = json.load(f)
    except Exception as e:
        return ValidationResult(is_valid=False, errors=[f"Failed to load manifest: {e}"])

    base_dir = manifest_path.parent
    tables = {}
    
    # Load tables from the manifest. Prefer parquet if available for speed.
    # Group files by table
    table_files = {}
    for f_meta in manifest.get("files", []):
        t_name = f_meta["table"]
        if t_name not in table_files:
            table_files[t_name] = {}
        table_files[t_name][f_meta["format"]] = f_meta["file"]

    for table_name, formats in table_files.items():
        try:
            if "parquet" in formats:
                tables[table_name] = pd.read_parquet(base_dir / formats["parquet"])
            elif "csv" in formats:
                tables[table_name] = pd.read_csv(base_dir / formats["csv"])
            else:
                logger.warning(f"No supported format found for table {table_name}")
        except Exception as e:
            return ValidationResult(is_valid=False, errors=[f"Failed to load table {table_name}: {e}"])

    result = ValidationResult(is_valid=True)
    
    # 1. FK Integrity Checks
    fk_errors = _check_fk_integrity(tables)
    result.errors.extend(fk_errors)
    if fk_errors:
        result.is_valid = False

    # 2. Distribution Sanity Checks
    dist_metrics = _check_distributions(tables)
    result.metrics["distributions"] = dist_metrics

    # 3. Aggregate Metrics
    agg_metrics = _calculate_aggregate_metrics(tables)
    result.metrics["aggregates"] = agg_metrics

    # 4. Correlation Sanity
    correlation_metrics = _check_correlations(tables)
    result.metrics["correlations"] = correlation_metrics

    # Generate Markdown Report
    report_path = base_dir / "validation_report.md"
    _generate_markdown_report(result, report_path, manifest)
    
    return result

def _check_fk_integrity(tables: dict[str, pd.DataFrame]) -> list[str]:
    """Check foreign key integrity across all tables."""
    errors = []
    for source_table, mappings in FK_MAPPINGS.items():
        if source_table not in tables:
            continue
        
        df_src = tables[source_table]
        for src_col, target_table, target_col in mappings:
            if target_table not in tables:
                # If target table is missing, we can't check
                continue
                
            df_target = tables[target_table]
            
            # Get unique non-null values from source
            src_values = df_src[src_col].dropna().unique()
            if len(src_values) == 0:
                continue
                
            # Get unique values from target PK
            target_values = set(df_target[target_col].unique())
            
            # Check for orphans
            orphans = [v for v in src_values if v not in target_values]
            if orphans:
                errors.append(
                    f"FK Integrity Error: {source_table}.{src_col} has {len(orphans)} orphaned values "
                    f"not found in {target_table}.{target_col}. Example: {orphans[:3]}"
                )
                
    return errors

def _check_distributions(tables: dict[str, pd.DataFrame]) -> dict[str, Any]:
    """Calculate p50/p95/p99 for key numeric columns."""
    metrics = {}
    
    # Transaction amounts
    if "fact_transaction" in tables:
        df = tables["fact_transaction"]
        metrics["transaction_amounts"] = {
            "p50": float(df["gross_amount"].quantile(0.5)),
            "p95": float(df["gross_amount"].quantile(0.95)),
            "p99": float(df["gross_amount"].quantile(0.99)),
        }
        
    # Dispute amounts
    if "fact_dispute" in tables:
        df = tables["fact_dispute"]
        metrics["dispute_amounts"] = {
            "p50": float(df["dispute_amount"].quantile(0.5)),
            "p95": float(df["dispute_amount"].quantile(0.95)),
            "p99": float(df["dispute_amount"].quantile(0.99)),
        }

    # Refund amounts
    if "fact_refund" in tables:
        df = tables["fact_refund"]
        metrics["refund_amounts"] = {
            "p50": float(df["refund_amount"].quantile(0.5)),
            "p95": float(df["refund_amount"].quantile(0.95)),
            "p99": float(df["refund_amount"].quantile(0.99)),
        }

    return metrics

def _calculate_aggregate_metrics(tables: dict[str, pd.DataFrame]) -> dict[str, float]:
    """Calculate high-level aggregate metrics."""
    metrics = {}
    
    if "fact_transaction" in tables:
        total_txns = len(tables["fact_transaction"])
        
        if "fact_dispute" in tables:
            metrics["dispute_rate"] = len(tables["fact_dispute"]) / total_txns if total_txns > 0 else 0
            
        if "fact_refund" in tables:
            metrics["refund_rate"] = len(tables["fact_refund"]) / total_txns if total_txns > 0 else 0
            
        # Block rate (declined txns)
        declined_txns = len(tables["fact_transaction"][tables["fact_transaction"]["status"] == "declined"])
        metrics["block_rate"] = declined_txns / total_txns if total_txns > 0 else 0
        
    metrics["return_rate"] = 0.0  # Documented as N/A but placeholder for consistency
    
    return metrics

def _check_correlations(tables: dict[str, pd.DataFrame]) -> dict[str, Any]:
    """Verify expected correlations in the data."""
    correlations = {}
    
    if "fact_transaction" in tables:
        df = tables["fact_transaction"]
        
        # Risk Tier Correlation: Higher risk tier -> Higher decline/emulator/fraud
        # Group by risk tier and calculate rates
        risk_stats = df.groupby("risk_tier").agg({
            "status": lambda x: (x == "declined").mean(),
            "is_emulator": "mean",
            "is_fraud_flagged": "mean"
        }).rename(columns={
            "status": "decline_rate",
            "is_emulator": "emulator_rate",
            "is_fraud_flagged": "fraud_rate"
        }).to_dict(orient="index")
        
        correlations["risk_tier_correlation"] = risk_stats
        
    return correlations

def _generate_markdown_report(result: ValidationResult, out_path: Path, manifest: dict[str, Any]) -> None:
    """Write the validation result to a Markdown file."""
    lines = [
        "# Synthetic Data Validation Report",
        f"**Status**: {'✅ PASS' if result.is_valid else '❌ FAIL'}",
        f"**Timestamp**: {manifest.get('generation_timestamp', 'Unknown')}",
        f"**Content Hash**: `{manifest.get('content_hash', 'N/A')}`",
        "",
        "## Summary Metrics",
    ]
    
    aggs = result.metrics.get("aggregates", {})
    lines.extend([
        f"- **Dispute Rate**: {aggs.get('dispute_rate', 0):.2%}",
        f"- **Refund Rate**: {aggs.get('refund_rate', 0):.2%}",
        f"- **Block Rate**: {aggs.get('block_rate', 0):.2%}",
        "- **Return Rate**: N/A"
    ])
    
    lines.append("\n## Distribution Sanity (p50 / p95 / p99)")
    dists = result.metrics.get("distributions", {})
    for name, stats in dists.items():
        lines.append(f"- **{name.replace('_', ' ').title()}**: ${stats['p50']:.2f} / ${stats['p95']:.2f} / ${stats['p99']:.2f}")
        
    lines.append("\n## Risk Correlation Analysis")
    corrs = result.metrics.get("correlations", {}).get("risk_tier_correlation", {})
    if corrs:
        lines.append("| Risk Tier | Decline Rate | Emulator Rate | Fraud Rate |")
        lines.append("|-----------|--------------|---------------|------------|")
        # Ensure consistent order
        for tier in ["low", "medium", "high", "critical"]:
            if tier in corrs:
                s = corrs[tier]
                lines.append(f"| {tier.capitalize()} | {s['decline_rate']:.2%} | {s['emulator_rate']:.2%} | {s['fraud_rate']:.2%} |")

    if result.errors:
        lines.append("\n## Errors")
        for err in result.errors:
            lines.append(f"- ❌ {err}")
            
    if result.warnings:
        lines.append("\n## Warnings")
        for warn in result.warnings:
            lines.append(f"- ⚠️ {warn}")

    with open(out_path, "w") as f:
        f.write("\n".join(lines))
    
    logger.info(f"Validation report written to {out_path}")
