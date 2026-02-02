from dataclasses import dataclass
from typing import Any, Dict, Optional

from common.config.env import get_env_str
from dal.query_target_config import QueryTargetConfigStatus
from dal.query_target_config_store import QueryTargetConfigStore


@dataclass(frozen=True)
class QueryTargetRuntimeConfig:
    """Normalized runtime config for a UI-selected query target."""

    provider: str
    metadata: Dict[str, Any]
    auth: Dict[str, Any]
    guardrails: Dict[str, Any]
    status: QueryTargetConfigStatus


async def load_query_target_runtime_config() -> Optional[QueryTargetRuntimeConfig]:
    """Load pending/active query-target config from control-plane store."""
    if not await QueryTargetConfigStore.init():
        return None

    pending = await QueryTargetConfigStore.get_pending()
    record = pending or await QueryTargetConfigStore.get_active()
    if not record:
        return None
    return QueryTargetRuntimeConfig(
        provider=record.provider,
        metadata=record.metadata,
        auth=record.auth,
        guardrails=record.guardrails,
        status=record.status,
    )


def resolve_secret_ref(auth: Dict[str, Any], fallback_env: str) -> Optional[str]:
    """Resolve secret via env reference or fallback."""
    secret_ref = auth.get("secret_ref")
    if isinstance(secret_ref, str) and secret_ref.startswith("env:"):
        return get_env_str(secret_ref[len("env:") :])
    if secret_ref:
        return None
    return get_env_str(fallback_env)


def guardrail_int(value: Optional[Any], default: int) -> int:
    """Coerce optional guardrail int value with a fallback default."""
    if isinstance(value, int) and value >= 0:
        return value
    return default


def guardrail_bool(value: Optional[Any], default: bool) -> bool:
    """Coerce optional guardrail bool value with a fallback default."""
    if isinstance(value, bool):
        return value
    return default


def get_optional_int(value: Optional[Any], default: int) -> int:
    """Convert an optional integer-like value with default fallback."""
    if value is None:
        return default
    return int(value)


def build_postgres_params(metadata: Dict[str, Any], auth: Dict[str, Any], default_port: int):
    """Build Postgres-style connection params from UI metadata/auth."""
    password = resolve_secret_ref(auth, "DB_PASS")
    if not password:
        raise ValueError("Missing database password reference.")
    return {
        "host": metadata["host"],
        "port": get_optional_int(metadata.get("port"), default_port),
        "db_name": metadata["db_name"],
        "user": metadata["user"],
        "password": password,
    }


def build_mysql_params(metadata: Dict[str, Any], auth: Dict[str, Any]):
    """Build MySQL-style connection params from UI metadata/auth."""
    password = resolve_secret_ref(auth, "DB_PASS")
    return {
        "host": metadata["host"],
        "port": get_optional_int(metadata.get("port"), 3306),
        "db_name": metadata["db_name"],
        "user": metadata["user"],
        "password": password,
    }


def build_clickhouse_params(metadata: Dict[str, Any], auth: Dict[str, Any]):
    """Build ClickHouse connection params from UI metadata/auth."""
    password = resolve_secret_ref(auth, "CLICKHOUSE_PASS") or ""
    secure = bool(metadata.get("secure", False))
    return {
        "host": metadata["host"],
        "port": get_optional_int(metadata.get("port"), 9000),
        "database": metadata["database"],
        "user": metadata.get("user") or "default",
        "password": password,
        "secure": secure,
    }
