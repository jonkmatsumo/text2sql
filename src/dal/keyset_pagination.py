import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Protocol, runtime_checkable

import sqlglot
from sqlglot import exp

KEYSET_REQUIRES_STABLE_TIEBREAKER = "KEYSET_REQUIRES_STABLE_TIEBREAKER"
KEYSET_ORDER_MISMATCH = "KEYSET_ORDER_MISMATCH"
KEYSET_ORDER_COLUMN_NOT_FOUND = "KEYSET_ORDER_COLUMN_NOT_FOUND"
KEYSET_TIEBREAKER_NULLABLE = "KEYSET_TIEBREAKER_NULLABLE"
KEYSET_TIEBREAKER_NOT_UNIQUE = "KEYSET_TIEBREAKER_NOT_UNIQUE"


@runtime_checkable
class SchemaInfoProvider(Protocol):
    """Minimal schema metadata contract used by keyset validation."""

    def has_column(self, table: str, col: str) -> bool:
        """Return True when the table exposes the requested column."""
        ...

    def is_nullable(self, table: str, col: str) -> Optional[bool]:
        """Return nullability for table.column when known."""
        ...

    def is_unique_key(self, table: str, col_set: List[str]) -> Optional[bool]:
        """Return whether table col_set is a unique key when known."""
        ...


class StaticSchemaInfoProvider:
    """In-memory schema provider for keyset validation."""

    def __init__(
        self,
        by_table: Dict[str, Dict[str, Dict[str, Any]]],
        *,
        unique_keys_by_table: Optional[Dict[str, List[List[str]]]] = None,
    ) -> None:
        """Normalize and store table/column metadata."""
        self._by_table: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._unique_keys_by_table: Dict[str, List[frozenset[str]]] = {}
        self._tables_with_uniqueness_info: set[str] = set()
        for table_name, columns in (by_table or {}).items():
            normalized_table = _normalize_identifier(table_name)
            if not normalized_table:
                continue
            normalized_columns: Dict[str, Dict[str, Any]] = {}
            for column_name, payload in (columns or {}).items():
                normalized_column = _normalize_identifier(column_name)
                if not normalized_column:
                    continue
                normalized_payload = payload if isinstance(payload, dict) else {}
                normalized_columns[normalized_column] = normalized_payload
                if any(
                    unique_flag in normalized_payload
                    for unique_flag in (
                        "is_unique",
                        "unique",
                        "is_primary_key",
                        "primary_key",
                        "is_pk",
                    )
                ):
                    self._tables_with_uniqueness_info.add(normalized_table)
                if any(
                    _is_truthy_metadata_flag(normalized_payload.get(unique_flag))
                    for unique_flag in (
                        "is_unique",
                        "unique",
                        "is_primary_key",
                        "primary_key",
                        "is_pk",
                    )
                ):
                    self._add_unique_key(normalized_table, [normalized_column])
            if normalized_columns:
                self._by_table[normalized_table] = normalized_columns

        for table_name, unique_keys in (unique_keys_by_table or {}).items():
            for normalized_table in _candidate_table_names(table_name):
                self._tables_with_uniqueness_info.add(normalized_table)
                for unique_key in unique_keys or []:
                    self._add_unique_key(normalized_table, unique_key)

    @classmethod
    def from_column_metadata(
        cls,
        column_metadata: Optional[Dict[str, Dict[str, Any]]],
        *,
        table_names: Optional[List[str]] = None,
        unique_keys_by_table: Optional[Dict[str, List[List[str]]]] = None,
    ) -> "StaticSchemaInfoProvider":
        """Build a provider from legacy flat keyset metadata."""
        by_table: Dict[str, Dict[str, Dict[str, Any]]] = {}
        metadata = column_metadata or {}
        normalized_tables = [_normalize_identifier(t) for t in (table_names or []) if t]
        single_table = normalized_tables[0] if len(normalized_tables) == 1 else None

        for raw_key, payload in metadata.items():
            normalized_key = _normalize_identifier(raw_key)
            if not normalized_key:
                continue
            table_name = ""
            column_name = normalized_key
            if "." in normalized_key:
                table_name, column_name = normalized_key.rsplit(".", 1)
            elif single_table:
                table_name = single_table
            else:
                continue

            if not table_name or not column_name:
                continue
            by_table.setdefault(table_name, {})[column_name] = (
                payload if isinstance(payload, dict) else {}
            )
            short_table = _normalize_table_name(table_name)
            if short_table and short_table != table_name:
                by_table.setdefault(short_table, {})[column_name] = (
                    payload if isinstance(payload, dict) else {}
                )

        return cls(by_table, unique_keys_by_table=unique_keys_by_table)

    def has_column(self, table: str, col: str) -> bool:
        """Return True when table.column is known in metadata."""
        return self._lookup_column_payload(table, col) is not None

    def is_nullable(self, table: str, col: str) -> Optional[bool]:
        """Return nullability when available for table.column."""
        payload = self._lookup_column_payload(table, col)
        if payload is None:
            return None
        raw_nullable = payload.get("nullable")
        if raw_nullable is None:
            raw_nullable = payload.get("is_nullable")
        if raw_nullable is None:
            return None
        return _is_truthy_metadata_flag(raw_nullable)

    def is_unique_key(self, table: str, col_set: List[str]) -> Optional[bool]:
        """Return uniqueness for single/composite keys when present in metadata."""
        normalized_col_set = [_normalize_identifier(col) for col in (col_set or []) if col]
        if not normalized_col_set:
            return None

        saw_uniqueness_info = False
        unique_keys: List[frozenset[str]] = []
        for candidate_table in _candidate_table_names(table):
            if candidate_table in self._tables_with_uniqueness_info:
                saw_uniqueness_info = True
            unique_keys.extend(self._unique_keys_by_table.get(candidate_table, []))

        if not saw_uniqueness_info and not unique_keys:
            return None
        target = frozenset(normalized_col_set)
        return target in unique_keys

    def _lookup_column_payload(self, table: str, col: str) -> Optional[Dict[str, Any]]:
        normalized_column = _normalize_identifier(col)
        if not normalized_column:
            return None
        for candidate_table in _candidate_table_names(table):
            table_columns = self._by_table.get(candidate_table)
            if not table_columns:
                continue
            payload = table_columns.get(normalized_column)
            if payload is not None:
                return payload
        return None

    def _add_unique_key(self, table: str, columns: List[str]) -> None:
        normalized_columns = [_normalize_identifier(col) for col in columns if col]
        normalized_columns = [col for col in normalized_columns if col]
        if not normalized_columns:
            return
        unique_set = frozenset(normalized_columns)
        if not unique_set:
            return
        self._unique_keys_by_table.setdefault(table, [])
        if unique_set not in self._unique_keys_by_table[table]:
            self._unique_keys_by_table[table].append(unique_set)


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
    explicit_nulls_order: bool


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
        if postgres_null_semantics:
            # Postgres defaults:
            # ASC: NULLS LAST
            # DESC: NULLS FIRST
            default_nulls_first = descending
            nulls_first = explicit_nulls_first is True
            explicit_nulls_order = nulls_first != default_nulls_first
        elif explicit_nulls_first is None:
            # Other providers remain conservative unless NULL ordering is explicit.
            nulls_first = False
            explicit_nulls_order = False
        else:
            nulls_first = explicit_nulls_first is True
            explicit_nulls_order = True

        # Try to resolve alias if 'this' is a Column and matches a projection alias
        alias = None
        if isinstance(this, exp.Column):
            alias = this.name

        keys.append(
            KeysetOrderKey(
                expression=this,
                alias=alias,
                descending=descending,
                nulls_first=nulls_first,
                explicit_nulls_order=explicit_nulls_order,
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
    schema_info: Optional[SchemaInfoProvider] = None,
) -> None:
    """Fail closed unless ORDER BY terminates in a stable deterministic tie-breaker."""
    if not order_keys:
        raise ValueError(
            f"{KEYSET_REQUIRES_STABLE_TIEBREAKER}: ORDER BY must include a stable tie-breaker."
        )

    if schema_info is not None:
        resolved_order_columns = _resolve_order_columns_with_schema(
            order_keys=order_keys,
            table_names=table_names,
            schema_info=schema_info,
        )
        tie_key = order_keys[-1]
        tie_table, tie_column = resolved_order_columns[-1]

        is_nullable = schema_info.is_nullable(tie_table, tie_column)
        if is_nullable is True and not tie_key.explicit_nulls_order:
            raise ValueError(
                f"{KEYSET_TIEBREAKER_NULLABLE}: "
                "Final ORDER BY key must be NOT NULL unless NULLS FIRST/LAST is explicit."
            )

        saw_uniqueness_info = False
        for suffix_table, suffix_columns in _candidate_unique_suffixes(resolved_order_columns):
            is_unique = schema_info.is_unique_key(suffix_table, suffix_columns)
            if is_unique is None:
                continue
            saw_uniqueness_info = True
            if is_unique is True:
                return

        if saw_uniqueness_info:
            raise ValueError(
                f"{KEYSET_TIEBREAKER_NOT_UNIQUE}: "
                "ORDER BY suffix must include a unique key for stable keyset pagination."
            )
        if _is_legacy_allowed_tiebreaker(tie_column, table_names=table_names, allowlist=allowlist):
            return
        raise ValueError(
            f"{KEYSET_REQUIRES_STABLE_TIEBREAKER}: "
            "Final ORDER BY key must be id/<table>_id or allowlisted."
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

    if not _is_legacy_allowed_tiebreaker(column_name, table_names=table_names, allowlist=allowlist):
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


def _candidate_table_names(table_name: str) -> List[str]:
    normalized = _normalize_identifier(table_name)
    if not normalized:
        return []
    short_name = _normalize_table_name(normalized)
    if short_name and short_name != normalized:
        return [normalized, short_name]
    return [normalized]


def _resolve_schema_table_for_column(
    *,
    raw_table_name: Any,
    column_name: str,
    table_names: Optional[List[str]],
    schema_info: SchemaInfoProvider,
) -> Optional[str]:
    candidates: List[str] = []
    explicit_table = _normalize_identifier(raw_table_name)
    explicit_candidates: List[str] = []
    if explicit_table:
        explicit_candidates = _candidate_table_names(explicit_table)
        candidates.extend(explicit_candidates)

    for table_name in table_names or []:
        for candidate in _candidate_table_names(table_name):
            if candidate not in candidates:
                candidates.append(candidate)

    matches: List[str] = []
    for candidate in candidates:
        if schema_info.has_column(candidate, column_name):
            matches.append(candidate)
    if not matches:
        return None

    for explicit_candidate in explicit_candidates:
        if explicit_candidate in matches:
            return explicit_candidate
    if len(matches) == 1:
        return matches[0]
    return None


def _resolve_order_columns_with_schema(
    *,
    order_keys: List[KeysetOrderKey],
    table_names: Optional[List[str]],
    schema_info: SchemaInfoProvider,
) -> List[tuple[str, str]]:
    resolved: List[tuple[str, str]] = []
    for key in order_keys:
        if not isinstance(key.expression, exp.Column):
            raise ValueError(
                f"{KEYSET_ORDER_COLUMN_NOT_FOUND}: ORDER BY keys must reference base columns."
            )
        column_name = _normalize_identifier(key.expression.name)
        if not column_name:
            raise ValueError(
                f"{KEYSET_ORDER_COLUMN_NOT_FOUND}: ORDER BY includes an invalid column reference."
            )

        table_name = _resolve_schema_table_for_column(
            raw_table_name=key.expression.table,
            column_name=column_name,
            table_names=table_names,
            schema_info=schema_info,
        )
        if not table_name:
            raise ValueError(
                f"{KEYSET_ORDER_COLUMN_NOT_FOUND}: ORDER BY column must exist in schema metadata."
            )
        resolved.append((table_name, column_name))
    return resolved


def _candidate_unique_suffixes(
    resolved_order_columns: List[tuple[str, str]],
) -> List[tuple[str, List[str]]]:
    suffixes: List[tuple[str, List[str]]] = []
    for start_idx in range(len(resolved_order_columns) - 1, -1, -1):
        suffix = resolved_order_columns[start_idx:]
        if not suffix:
            continue
        suffix_table = suffix[0][0]
        if any(table_name != suffix_table for table_name, _ in suffix):
            continue
        suffix_columns = [column_name for _, column_name in suffix]
        suffixes.append((suffix_table, suffix_columns))
    return suffixes


def _is_legacy_allowed_tiebreaker(
    column_name: str,
    *,
    table_names: Optional[List[str]],
    allowlist: Optional[set[str]],
) -> bool:
    allowed_columns = {"id"}
    for table_name in table_names or []:
        normalized_table = _normalize_table_name(table_name)
        if normalized_table:
            allowed_columns.add(f"{normalized_table}_id")
    for name in allowlist or set():
        normalized_name = _normalize_identifier(name)
        if normalized_name:
            allowed_columns.add(normalized_name)
    return column_name in allowed_columns


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
