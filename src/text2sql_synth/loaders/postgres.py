"""Postgres loader for synthetic data using manifest and COPY command."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from text2sql_synth import schema

logger = logging.getLogger(__name__)


def _get_column_type(column_name: str) -> str:
    """Infer Postgres type from column name."""
    col = column_name.lower()
    text_exact = {
        "id",
        "email",
        "phone",
        "currency",
        "channel",
        "status",
        "risk_tier",
        "device_id",
        "account_number",
        "auth_code",
    }
    text_suffixes = ("_id", "_code", "_name", "_reason")

    if col in text_exact or col.endswith(text_suffixes):
        return "TEXT"

    if col.endswith(("_ts", "_at", "_date", "_since")) or col == "full_date":
        return "TIMESTAMP"

    if col.endswith(("_amount", "_score", "_fee", "_limit")) or col in {
        "seasonality_factor",
        "latitude",
        "longitude",
    }:
        return "NUMERIC"

    if col.startswith("is_") or col in {
        "is_active",
        "is_emulator",
        "is_fraud_flagged",
        "is_current",
        "is_partial",
        "is_chargeback",
        "merchant_responded",
        "evidence_submitted",
        "processing_fee_refunded",
    }:
        return "BOOLEAN"

    if (
        col.endswith("_count")
        or col.startswith("day_of_")
        or col.startswith("week_of_")
        or col
        in (
            "month",
            "year",
            "quarter",
            "date_key",
            "version_number",
            "popularity_score",
            "days_to_resolution",
        )
    ):
        return "INTEGER"

    return "TEXT"


def _quote_id(identifier: str) -> str:
    """Quote a Postgres identifier."""
    return '"' + identifier.replace('"', '""') + '"'


def load_from_manifest(
    manifest_path: str | Path,
    dsn: str,
    target_schema: str = "public",
    table_prefix: str = "",
    truncate: bool = True,
) -> None:
    """Load synthetic data into Postgres driven by a manifest.json.

    Args:
        manifest_path: Path to the manifest.json file.
        dsn: Postgres connection string.
        target_schema: Target schema in Postgres.
        table_prefix: Optional prefix for table names.
        truncate: Whether to truncate tables before loading.
    """
    try:
        import psycopg
    except ImportError:
        try:
            import psycopg2 as psycopg
        except ImportError:
            raise ImportError("psycopg (v3) or psycopg2 is required for Postgres loading.")

    manifest_path = Path(manifest_path)
    base_dir = manifest_path.parent

    with open(manifest_path) as f:
        manifest = json.load(f)

    table_metadata = manifest.get("tables", {})
    files = manifest.get("files", [])

    # Map table name to its CSV file relative to manifest
    csv_files = {f["table"]: f["file"] for f in files if f["format"] == "csv"}

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            # Create schema if not exists
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_id(target_schema)}")

            # Use GENERATION_ORDER to ensure FK-safe loading if we had FKs,
            # and to maintain a consistent order.
            for table_name in schema.GENERATION_ORDER:
                if table_name not in table_metadata or table_name not in csv_files:
                    continue

                full_table_name = f"{table_prefix}{table_name}"
                csv_rel_path = csv_files[table_name]
                csv_abs_path = base_dir / csv_rel_path

                if not csv_abs_path.exists():
                    logger.warning("CSV file not found for table %s: %s", table_name, csv_abs_path)
                    continue

                # Get columns from manifest
                columns = table_metadata[table_name]["columns"]

                # Build CREATE TABLE statement
                col_defs = []
                for col in columns:
                    col_type = _get_column_type(col)
                    col_defs.append(f"{_quote_id(col)} {col_type}")

                create_sql = f"""
                CREATE TABLE IF NOT EXISTS {_quote_id(target_schema)}.{_quote_id(full_table_name)} (
                    {", ".join(col_defs)}
                )
                """
                logger.info(f"Creating table {target_schema}.{full_table_name} if not exists...")
                cur.execute(create_sql)

                # Truncate
                if truncate:
                    logger.info(f"Truncating table {target_schema}.{full_table_name}...")
                    cur.execute(
                        f"TRUNCATE TABLE {_quote_id(target_schema)}."
                        f"{_quote_id(full_table_name)} CASCADE"
                    )

                # COPY command
                # We use the raw COPY command which requires the file to be accessible
                # by the DB server
                # or we use COPY FROM STDIN which works over the connection.
                # psycopg (v3) copy:
                # with cur.copy(
                #     f"COPY {schema}.{table} ({cols}) FROM STDIN WITH (FORMAT CSV, HEADER)"
                # ) as copy:
                #     with open(path, "rb") as f:
                #         copy.write(f.read())

                logger.info(f"Loading data into {target_schema}.{full_table_name} via COPY...")
                quoted_cols = ", ".join(_quote_id(c) for c in columns)
                copy_stmt = (
                    f"COPY {_quote_id(target_schema)}.{_quote_id(full_table_name)} "
                    f"({quoted_cols}) FROM STDIN WITH (FORMAT CSV, HEADER)"
                )

                if hasattr(cur, "copy"):
                    # psycopg 3
                    with cur.copy(copy_stmt) as copy:
                        with open(csv_abs_path, "rb") as f:
                            while data := f.read(8192):
                                copy.write(data)
                else:
                    # psycopg 2
                    with open(csv_abs_path, "r") as f:
                        cur.copy_expert(copy_stmt, f)

            conn.commit()

    logger.info("Postgres load complete.")
