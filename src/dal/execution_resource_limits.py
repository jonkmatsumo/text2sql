"""Unified execution resource limits for SQL tool boundaries."""

from __future__ import annotations

from dataclasses import dataclass

from common.config.env import get_env_bool, get_env_int
from dal.util.row_limits import get_sync_max_rows

_DEFAULT_MAX_BYTES = 2 * 1024 * 1024
_DEFAULT_MAX_EXECUTION_MS = 30_000
_DEFAULT_MAX_ROWS_FALLBACK = 1_000


@dataclass(frozen=True)
class ExecutionResourceLimits:
    """Typed execution-boundary settings for resource containment."""

    max_rows: int
    max_bytes: int
    max_execution_ms: int
    enforce_row_limit: bool
    enforce_byte_limit: bool
    enforce_timeout: bool

    @classmethod
    def from_env(cls) -> "ExecutionResourceLimits":
        """Load and validate execution-boundary limits from environment."""
        default_rows = get_sync_max_rows() or _DEFAULT_MAX_ROWS_FALLBACK
        limits = cls(
            max_rows=int(get_env_int("EXECUTION_RESOURCE_MAX_ROWS", default_rows) or 0),
            max_bytes=int(
                get_env_int(
                    "EXECUTION_RESOURCE_MAX_BYTES",
                    get_env_int("MCP_JSON_PAYLOAD_LIMIT_BYTES", _DEFAULT_MAX_BYTES),
                )
                or 0
            ),
            max_execution_ms=int(
                get_env_int("EXECUTION_RESOURCE_MAX_EXECUTION_MS", _DEFAULT_MAX_EXECUTION_MS) or 0
            ),
            enforce_row_limit=bool(get_env_bool("EXECUTION_RESOURCE_ENFORCE_ROW_LIMIT", True)),
            enforce_byte_limit=bool(get_env_bool("EXECUTION_RESOURCE_ENFORCE_BYTE_LIMIT", True)),
            enforce_timeout=bool(get_env_bool("EXECUTION_RESOURCE_ENFORCE_TIMEOUT", True)),
        )
        limits.validate()
        return limits

    def validate(self) -> None:
        """Fail closed on invalid or unsafe limit configuration."""
        if self.enforce_row_limit and self.max_rows <= 0:
            raise ValueError(
                "EXECUTION_RESOURCE_ENFORCE_ROW_LIMIT=true requires "
                "EXECUTION_RESOURCE_MAX_ROWS > 0."
            )
        if self.enforce_byte_limit and self.max_bytes <= 0:
            raise ValueError(
                "EXECUTION_RESOURCE_ENFORCE_BYTE_LIMIT=true requires "
                "EXECUTION_RESOURCE_MAX_BYTES > 0."
            )
        if self.enforce_timeout and self.max_execution_ms <= 0:
            raise ValueError(
                "EXECUTION_RESOURCE_ENFORCE_TIMEOUT=true requires "
                "EXECUTION_RESOURCE_MAX_EXECUTION_MS > 0."
            )
