from typing import Any, Dict, List, Tuple

from common.config.env import get_env_bool


class QueryTargetValidationError(ValueError):
    """Raised when a query-target config payload is invalid."""


SUPPORTED_PROVIDERS = {
    "postgres",
    "cockroachdb",
    "mysql",
    "redshift",
    "sqlite",
    "duckdb",
    "snowflake",
    "bigquery",
    "athena",
    "databricks",
    "clickhouse",
}

LOCAL_ONLY_PROVIDERS = {"sqlite", "duckdb"}

SECRET_KEYWORDS = (
    "password",
    "token",
    "secret",
    "private_key",
    "access_key",
    "client_secret",
    "credentials",
)

ALLOWED_AUTH_KEYS = {"secret_ref", "identity_profile"}


PROVIDER_REQUIRED_FIELDS: Dict[str, List[str]] = {
    "postgres": ["host", "db_name", "user"],
    "cockroachdb": ["host", "db_name", "user"],
    "mysql": ["host", "db_name", "user"],
    "redshift": ["host", "db_name", "user"],
    "sqlite": ["path"],
    "duckdb": ["path"],
    "snowflake": ["account", "user", "warehouse", "database", "schema"],
    "bigquery": ["project", "dataset"],
    "athena": ["region", "workgroup", "output_location", "database"],
    "databricks": ["host", "warehouse_id", "catalog", "schema"],
    "clickhouse": ["host", "database"],
}

PROVIDER_ALLOWED_FIELDS: Dict[str, List[str]] = {
    "postgres": ["host", "port", "db_name", "user"],
    "cockroachdb": ["host", "port", "db_name", "user"],
    "mysql": ["host", "port", "db_name", "user"],
    "redshift": ["host", "port", "db_name", "user"],
    "sqlite": ["path"],
    "duckdb": ["path", "read_only"],
    "snowflake": [
        "account",
        "user",
        "warehouse",
        "database",
        "schema",
        "role",
        "authenticator",
    ],
    "bigquery": ["project", "dataset", "location"],
    "athena": ["region", "workgroup", "output_location", "database"],
    "databricks": ["host", "warehouse_id", "catalog", "schema"],
    "clickhouse": ["host", "port", "database", "user", "secure"],
}

PROVIDER_ALLOWED_GUARDRAILS: Dict[str, List[str]] = {
    "postgres": ["max_rows"],
    "cockroachdb": ["max_rows"],
    "mysql": ["max_rows"],
    "redshift": ["max_rows"],
    "sqlite": ["max_rows"],
    "duckdb": ["max_rows", "query_timeout_seconds", "read_only"],
    "snowflake": [
        "query_timeout_seconds",
        "poll_interval_seconds",
        "max_rows",
        "warn_after_seconds",
    ],
    "bigquery": ["query_timeout_seconds", "poll_interval_seconds", "max_rows"],
    "athena": ["query_timeout_seconds", "poll_interval_seconds", "max_rows"],
    "databricks": ["query_timeout_seconds", "poll_interval_seconds", "max_rows"],
    "clickhouse": ["query_timeout_seconds", "max_rows"],
}


def validate_query_target_payload(
    provider: str,
    metadata: Dict[str, Any],
    auth: Dict[str, Any],
    guardrails: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Validate query-target config payload and return normalized sections."""
    normalized_provider = provider.strip().lower()
    if normalized_provider not in SUPPORTED_PROVIDERS:
        raise QueryTargetValidationError(
            f"Unsupported provider '{provider}'. Allowed: {sorted(SUPPORTED_PROVIDERS)}"
        )

    if normalized_provider in LOCAL_ONLY_PROVIDERS and not get_env_bool(
        "DAL_ALLOW_LOCAL_QUERY_TARGETS", False
    ):
        raise QueryTargetValidationError(
            f"Provider '{normalized_provider}' is restricted to local/dev mode."
        )

    _validate_no_secrets(metadata, "metadata")
    _validate_no_secrets(auth, "auth")
    _validate_allowed_keys(auth, ALLOWED_AUTH_KEYS, "auth")

    _validate_required_fields(metadata, PROVIDER_REQUIRED_FIELDS[normalized_provider])
    _validate_allowed_keys(metadata, PROVIDER_ALLOWED_FIELDS[normalized_provider], "metadata")

    allowed_guardrails = PROVIDER_ALLOWED_GUARDRAILS[normalized_provider]
    _validate_allowed_keys(guardrails, allowed_guardrails, "guardrails")
    _validate_guardrails(guardrails)

    return metadata, auth, guardrails


def _validate_required_fields(metadata: Dict[str, Any], required: List[str]) -> None:
    missing = [field for field in required if not metadata.get(field)]
    if missing:
        missing_list = ", ".join(missing)
        raise QueryTargetValidationError(f"Missing required fields: {missing_list}")


def _validate_allowed_keys(
    payload: Dict[str, Any], allowed: List[str] | set[str], label: str
) -> None:
    allowed_set = set(allowed)
    extra = [key for key in payload.keys() if key not in allowed_set]
    if extra:
        raise QueryTargetValidationError(f"Unsupported {label} fields: {', '.join(sorted(extra))}")


def _validate_no_secrets(payload: Dict[str, Any], label: str) -> None:
    for key in payload.keys():
        lower = key.lower()
        if any(keyword in lower for keyword in SECRET_KEYWORDS):
            raise QueryTargetValidationError(
                f"Field '{key}' is not allowed in {label}; secrets must be managed externally."
            )


def _validate_guardrails(guardrails: Dict[str, Any]) -> None:
    for key, value in guardrails.items():
        if key in {
            "max_rows",
            "query_timeout_seconds",
            "poll_interval_seconds",
            "warn_after_seconds",
        }:
            if not isinstance(value, int) or value < 0:
                raise QueryTargetValidationError(
                    f"Guardrail '{key}' must be a non-negative integer."
                )
        if key == "read_only" and not isinstance(value, bool):
            raise QueryTargetValidationError("Guardrail 'read_only' must be a boolean.")
