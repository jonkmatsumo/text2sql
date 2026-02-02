from dataclasses import dataclass
from typing import Any, Dict, Optional
from uuid import UUID

from common.config.env import get_env_str
from dal.query_target_config import QueryTargetConfigStatus
from dal.query_target_config_store import QueryTargetConfigStore


@dataclass(frozen=True)
class QueryTargetRuntimeConfig:
    """Normalized runtime config for a UI-selected query target."""

    id: UUID
    provider: str
    metadata: Dict[str, Any]
    auth: Dict[str, Any]
    guardrails: Dict[str, Any]
    status: QueryTargetConfigStatus


@dataclass(frozen=True)
class QueryTargetConfigSelection:
    """Pending/active query-target configs selected from control-plane."""

    pending: Optional[QueryTargetRuntimeConfig]
    active: Optional[QueryTargetRuntimeConfig]


async def load_query_target_config_selection() -> QueryTargetConfigSelection:
    """Load pending/active query-target configs from control-plane store."""
    if not await QueryTargetConfigStore.init():
        return QueryTargetConfigSelection(pending=None, active=None)

    pending_record = await QueryTargetConfigStore.get_pending()
    active_record = await QueryTargetConfigStore.get_active()
    return QueryTargetConfigSelection(
        pending=_to_runtime_config(pending_record) if pending_record else None,
        active=_to_runtime_config(active_record) if active_record else None,
    )


def _to_runtime_config(record) -> QueryTargetRuntimeConfig:
    return QueryTargetRuntimeConfig(
        id=record.id,
        provider=record.provider,
        metadata=record.metadata,
        auth=record.auth,
        guardrails=record.guardrails,
        status=record.status,
    )


async def finalize_pending_config(
    pending: QueryTargetRuntimeConfig,
    active: Optional[QueryTargetRuntimeConfig],
    *,
    success: bool,
    error_message: Optional[str] = None,
) -> None:
    """Finalize pending config status after init attempt."""
    if not QueryTargetConfigStore.is_available():
        return

    if success:
        await QueryTargetConfigStore.set_status(
            pending.id,
            QueryTargetConfigStatus.ACTIVE,
            activated=True,
        )
        if active:
            await QueryTargetConfigStore.set_status(
                active.id,
                QueryTargetConfigStatus.INACTIVE,
                deactivated=True,
            )
        return

    await QueryTargetConfigStore.set_status(
        pending.id,
        QueryTargetConfigStatus.UNHEALTHY,
        error_code="init_failed",
        error_message=error_message,
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
