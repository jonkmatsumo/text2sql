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
    cursor: str, expected_fingerprint: str, secret: Optional[str] = None
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
        nulls_first = o.args.get("nulls_first") is True
        # If nulls_first is not specified, postgres defaults:
        # ASC: NULLS LAST
        # DESC: NULLS FIRST
        if o.args.get("nulls_first") is None:
            nulls_first = descending

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

    predicate = _build_keyset_predicate(order_keys, values)
    return expression.where(predicate)


def _build_keyset_predicate(keys: List[KeysetOrderKey], values: List[Any]) -> exp.Condition:
    """Recursively build the nested keyset predicate."""
    key = keys[0]
    val = values[0]

    val_exp = _to_exp_literal(val)

    # Comparison op based on direction
    op = exp.GT if not key.descending else exp.LT

    # Use the expression from the ORDER BY
    comp = op(this=key.expression.copy(), expression=val_exp)

    if len(keys) == 1:
        return comp

    eq = exp.EQ(this=key.expression.copy(), expression=val_exp)
    next_predicate = _build_keyset_predicate(keys[1:], values[1:])

    return exp.Or(this=comp, expression=exp.And(this=eq, expression=next_predicate))


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
