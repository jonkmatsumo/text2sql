import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

import sqlglot
from sqlglot import exp

KEYSET_REQUIRES_STABLE_TIEBREAKER = "KEYSET_REQUIRES_STABLE_TIEBREAKER"
KEYSET_ORDER_MISMATCH = "KEYSET_ORDER_MISMATCH"


@dataclass(frozen=True)
class KeysetCursorPayload:
    """Internal structure of the keyset cursor."""

    values: List[Any]
    keys: List[str]
    fingerprint: str


def encode_keyset_cursor(
    values: List[Any], keys: List[str], fingerprint: str, secret: Optional[str] = None
) -> str:
    """Encode keyset values and keys into an opaque base64 cursor."""
    payload = {
        "v": [_json_serializable(v) for v in values],
        "k": keys,
        "f": fingerprint,
    }
    if secret:
        payload["s"] = _calculate_signature(payload, secret)

    json_data = json.dumps(payload, sort_keys=True)
    return base64.urlsafe_b64encode(json_data.encode()).decode().rstrip("=")


def decode_keyset_cursor(
    cursor: str,
    expected_fingerprint: str,
    secret: Optional[str] = None,
    expected_keys: Optional[List[str]] = None,
) -> List[Any]:
    """Decode and validate a keyset cursor."""
    try:
        # Add padding if needed
        missing_padding = len(cursor) % 4
        if missing_padding:
            cursor += "=" * (4 - missing_padding)

        json_data = base64.urlsafe_b64decode(cursor).decode()
        payload = json.loads(json_data)

        if payload.get("f") != expected_fingerprint:
            raise ValueError("Invalid cursor: fingerprint mismatch.")
        if expected_keys is not None:
            raw_keys = payload.get("k", [])
            payload_keys = raw_keys if isinstance(raw_keys, list) else []
            if payload_keys != expected_keys:
                raise ValueError(f"Invalid cursor: {KEYSET_ORDER_MISMATCH}.")

        if secret:
            stored_sig = payload.get("s")
            payload_for_sig = {k: v for k, v in payload.items() if k != "s"}
            if not stored_sig or not hmac.compare_digest(
                stored_sig, _calculate_signature(payload_for_sig, secret)
            ):
                raise ValueError("Invalid cursor: signature mismatch.")

        return payload.get("v", [])
    except Exception as e:
        if isinstance(e, ValueError) and str(e).startswith("Invalid cursor:"):
            raise
        raise ValueError(f"Failed to decode cursor: {str(e)}")


def _json_serializable(obj: Any) -> Any:
    """Convert objects to JSON serializable formats."""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return str(obj)
    return obj


def _calculate_signature(payload: Dict[str, Any], secret: str) -> str:
    """Calculate HMAC signature for payload integrity."""
    json_data = json.dumps(payload, sort_keys=True)
    return hmac.new(secret.encode(), json_data.encode(), hashlib.sha256).hexdigest()


@dataclass(frozen=True)
class KeysetOrderKey:
    """Canonical representation of an ORDER BY key for keyset pagination."""

    expression: exp.Expression  # The parsed SQL expression
    alias: Optional[str]  # The alias if defined
    descending: bool
    nulls_first: bool


def extract_keyset_order_keys(sql: str, provider: str = "postgres") -> List[KeysetOrderKey]:
    """Extract and canonicalize ORDER BY keys from a SQL query."""
    dialect = sqlglot.Dialect.get(provider)
    postgres_null_semantics = _is_postgres_provider(provider)
    try:
        expressions = sqlglot.parse(sql, read=dialect)
    except Exception as e:
        raise ValueError(f"Failed to parse SQL: {str(e)}")

    if not expressions or len(expressions) != 1 or not isinstance(expressions[0], exp.Select):
        raise ValueError("Keyset pagination only supports a single SELECT statement.")

    expression = expressions[0]
    order = expression.args.get("order")
    if not order:
        return []

    keys = []
    for o in order.expressions:
        this = o.this
        # Reject nondeterministic expressions
        if _is_nondeterministic(this):
            raise ValueError(
                f"Nondeterministic ORDER BY expression not allowed: {this.sql(dialect)}"
            )

        descending = o.args.get("desc") is True
        explicit_nulls_first = o.args.get("nulls_first")
        if explicit_nulls_first is None:
            # Postgres defaults:
            # ASC: NULLS LAST
            # DESC: NULLS FIRST
            # Other providers remain conservative unless NULL ordering is explicit.
            nulls_first = descending if postgres_null_semantics else False
        else:
            nulls_first = explicit_nulls_first is True

        # Try to resolve alias if 'this' is a Column and matches a projection alias
        alias = None
        if isinstance(this, exp.Column):
            alias = this.name

        keys.append(
            KeysetOrderKey(
                expression=this, alias=alias, descending=descending, nulls_first=nulls_first
            )
        )

    return keys


def extract_keyset_table_names(sql: str, provider: str = "postgres") -> List[str]:
    """Extract base table names referenced by a SELECT query."""
    dialect = sqlglot.Dialect.get(provider)
    try:
        expression = sqlglot.parse_one(sql, read=dialect)
    except Exception as e:
        raise ValueError(f"Failed to parse SQL: {str(e)}")

    if not isinstance(expression, exp.Select):
        raise ValueError("Keyset pagination only supports a single SELECT statement.")

    cte_names = {
        _normalize_identifier(cte.alias_or_name)
        for cte in expression.find_all(exp.CTE)
        if cte.alias_or_name
    }
    table_names: List[str] = []
    seen: set[str] = set()
    for table in expression.find_all(exp.Table):
        table_name = _normalize_identifier(table.name)
        if not table_name or table_name in cte_names:
            continue
        schema_name = _normalize_identifier(table.db)
        full_name = f"{schema_name}.{table_name}" if schema_name else table_name
        if full_name in seen:
            continue
        seen.add(full_name)
        table_names.append(full_name)
    return table_names


def validate_stable_tiebreaker(
    order_keys: List[KeysetOrderKey],
    *,
    table_names: Optional[List[str]] = None,
    allowlist: Optional[set[str]] = None,
    column_metadata: Optional[Dict[str, Dict[str, Any]]] = None,
) -> None:
    """Fail closed unless ORDER BY terminates in a stable deterministic tie-breaker."""
    if not order_keys:
        raise ValueError(
            f"{KEYSET_REQUIRES_STABLE_TIEBREAKER}: ORDER BY must include a stable tie-breaker."
        )

    tie_key = order_keys[-1]
    if _is_nondeterministic(tie_key.expression):
        raise ValueError(
            f"{KEYSET_REQUIRES_STABLE_TIEBREAKER}: ORDER BY tie-breaker must be deterministic."
        )
    if not isinstance(tie_key.expression, exp.Column):
        raise ValueError(
            f"{KEYSET_REQUIRES_STABLE_TIEBREAKER}: Final ORDER BY key must be a plain column."
        )

    column_name = _normalize_identifier(tie_key.expression.name)
    if not column_name:
        raise ValueError(
            f"{KEYSET_REQUIRES_STABLE_TIEBREAKER}: Final ORDER BY column name is invalid."
        )

    metadata = column_metadata or {}
    if metadata:
        qualified_name = _normalize_identifier(tie_key.expression.table)
        metadata_key = column_name
        if qualified_name:
            metadata_key = f"{qualified_name}.{column_name}"
        column_info = metadata.get(metadata_key) or metadata.get(column_name)
        if not column_info:
            raise ValueError(
                f"{KEYSET_REQUIRES_STABLE_TIEBREAKER}: Unable to verify tie-breaker metadata."
            )

        is_nullable = _is_truthy_metadata_flag(
            column_info.get("nullable")
        ) or _is_truthy_metadata_flag(column_info.get("is_nullable"))
        if is_nullable:
            raise ValueError(
                f"{KEYSET_REQUIRES_STABLE_TIEBREAKER}: Final ORDER BY key must be NOT NULL."
            )

        is_unique = any(
            _is_truthy_metadata_flag(column_info.get(flag))
            for flag in ("is_unique", "unique", "is_primary_key", "primary_key", "is_pk")
        )
        if not is_unique:
            raise ValueError(
                f"{KEYSET_REQUIRES_STABLE_TIEBREAKER}: "
                "Final ORDER BY key must be unique or primary key."
            )
        return

    allowed_columns = {"id"}
    for table_name in table_names or []:
        normalized_table = _normalize_table_name(table_name)
        if normalized_table:
            allowed_columns.add(f"{normalized_table}_id")
    for name in allowlist or set():
        normalized_name = _normalize_identifier(name)
        if normalized_name:
            allowed_columns.add(normalized_name)

    if column_name not in allowed_columns:
        raise ValueError(
            f"{KEYSET_REQUIRES_STABLE_TIEBREAKER}: "
            "Final ORDER BY key must be id/<table>_id or allowlisted."
        )


def build_keyset_order_signature(order_keys: List[KeysetOrderKey]) -> List[str]:
    """Build a deterministic structural signature for ORDER BY parity checks."""
    signature: List[str] = []
    for key in order_keys:
        expression_sql = _normalize_sql_fragment(key.expression.sql())
        direction = "desc" if key.descending else "asc"
        nulls = "nulls_first" if key.nulls_first else "nulls_last"
        signature.append(f"{expression_sql}|{direction}|{nulls}")
    return signature


def apply_keyset_pagination(
    expression: exp.Select,
    order_keys: List[KeysetOrderKey],
    values: List[Any],
    provider: str = "postgres",
) -> exp.Select:
    """Rewrite a SELECT expression to include a keyset pagination predicate."""
    if not order_keys or not values:
        return expression

    if len(order_keys) != len(values):
        raise ValueError(
            f"Mismatch between order keys ({len(order_keys)}) and values ({len(values)}) count."
        )

    predicate = _build_keyset_predicate(order_keys, values, provider=provider)
    return expression.where(predicate)


def canonicalize_keyset_sql(expression: exp.Select, provider: str = "postgres") -> str:
    """Render keyset-rewritten SQL in a stable canonical string form."""
    dialect = sqlglot.Dialect.get(provider)
    rendered = expression.sql(dialect=dialect, pretty=False)
    return " ".join(rendered.split())


def _build_keyset_predicate(
    keys: List[KeysetOrderKey], values: List[Any], provider: str = "postgres"
) -> exp.Condition:
    """Recursively build the nested keyset predicate."""
    key = keys[0]
    val = values[0]

    comp = _build_order_comparison(key, val, provider=provider)

    if len(keys) == 1:
        return comp

    eq = _build_order_equality(key, val)
    next_predicate = _build_keyset_predicate(keys[1:], values[1:], provider=provider)

    tie_branch = exp.And(this=eq, expression=exp.Paren(this=next_predicate))
    return exp.Or(this=comp, expression=exp.Paren(this=tie_branch))


def _build_order_comparison(
    key: KeysetOrderKey, value: Any, provider: str = "postgres"
) -> exp.Condition:
    """Build keyset 'strictly after cursor' comparison for one ORDER BY key."""
    if _is_postgres_provider(provider):
        return _build_postgres_order_comparison(key, value)
    return _build_fail_closed_order_comparison(key, value)


def _build_postgres_order_comparison(key: KeysetOrderKey, value: Any) -> exp.Condition:
    """Build comparison semantics that match Postgres NULLS FIRST/LAST behavior."""
    key_exp = key.expression.copy()
    if value is None:
        # NULLS FIRST: non-null values follow nulls.
        # NULLS LAST: nothing follows null at this key.
        # Tie-breakers are handled by the equality branch.
        if key.nulls_first:
            return exp.Not(this=exp.Is(this=key_exp, expression=exp.Null()))
        return exp.Boolean(this=False)

    op = exp.GT if not key.descending else exp.LT
    comp: exp.Condition = op(this=key_exp, expression=_to_exp_literal(value))
    if not key.nulls_first:
        # For NULLS LAST, null rows are ordered after non-null cursor values.
        comp = exp.Or(
            this=comp, expression=exp.Is(this=key.expression.copy(), expression=exp.Null())
        )
    return comp


def _build_fail_closed_order_comparison(key: KeysetOrderKey, value: Any) -> exp.Condition:
    """Build conservative comparison semantics for non-Postgres providers."""
    if value is None:
        return exp.Boolean(this=False)
    op = exp.GT if not key.descending else exp.LT
    return op(this=key.expression.copy(), expression=_to_exp_literal(value))


def _build_order_equality(key: KeysetOrderKey, value: Any) -> exp.Condition:
    """Build equality clause used for lexicographic tie-break traversal."""
    key_exp = key.expression.copy()
    if value is None:
        return exp.Is(this=key_exp, expression=exp.Null())
    return exp.EQ(this=key_exp, expression=_to_exp_literal(value))


def _to_exp_literal(val: Any) -> exp.Expression:
    """Convert a Python value to a sqlglot literal expression."""
    if val is None:
        return exp.Null()
    if isinstance(val, bool):
        return exp.Boolean(this=val)
    if isinstance(val, (int, float)):
        return exp.Literal.number(str(val))
    if isinstance(val, str):
        return exp.Literal.string(val)
    # Datetime/Date handled as strings for now, most dialects accept ISO format
    return exp.Literal.string(str(val))


def get_keyset_values(row: Dict[str, Any], order_keys: List[KeysetOrderKey]) -> List[Any]:
    """Extract keyset values from a result row."""
    values = []
    for key in order_keys:
        # 1. Try alias
        if key.alias and key.alias in row:
            values.append(row[key.alias])
            continue

        # 2. Try column name
        if isinstance(key.expression, exp.Column) and key.expression.name in row:
            values.append(row[key.expression.name])
            continue

        # 3. Try expression SQL
        expr_sql = key.expression.sql().lower()
        found = False
        for k in row.keys():
            if k.lower() == expr_sql:
                values.append(row[k])
                found = True
                break
        if found:
            continue

        raise ValueError(
            f"Keyset column '{key.expression.sql()}' not found in result row. "
            "Please ensure it is projected in the SELECT list."
        )

    return values


def _is_nondeterministic(expression: exp.Expression) -> bool:
    """Check if an expression contains nondeterministic functions."""
    nondeterministic_funcs = {
        "RAND",
        "RANDOM",
        "UUID",
        "GEN_RANDOM_UUID",
        "NOW",
        "CURRENT_TIMESTAMP",
    }
    for func in expression.find_all(exp.Func, exp.Anonymous):
        name = func.this if isinstance(func, exp.Anonymous) else func.key
        if name and name.upper() in nondeterministic_funcs:
            return True
    return False


def _is_postgres_provider(provider: str) -> bool:
    """Return True when provider is PostgreSQL family."""
    return (provider or "").strip().lower() in {"postgres", "postgresql"}


def _normalize_identifier(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().strip('"').lower()


def _normalize_table_name(table_name: str) -> str:
    normalized = _normalize_identifier(table_name)
    return normalized.split(".")[-1] if normalized else ""


def _is_truthy_metadata_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    if isinstance(value, (int, float)):
        return bool(value)
    return False


def _normalize_sql_fragment(value: str) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.strip().lower().split())
