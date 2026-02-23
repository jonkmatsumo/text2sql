"""Postgres transaction sandbox utilities for tool execution."""

from __future__ import annotations

from dataclasses import dataclass
from types import TracebackType
from typing import Any, Optional, Type

from common.config.env import get_env_bool

SANDBOX_FAILURE_NONE = "NONE"
SANDBOX_FAILURE_QUERY_ERROR = "QUERY_ERROR"
SANDBOX_FAILURE_TIMEOUT = "TIMEOUT"
SANDBOX_FAILURE_ROLE_SWITCH_FAILURE = "ROLE_SWITCH_FAILURE"
SANDBOX_FAILURE_RESET_FAILURE = "RESET_FAILURE"
SANDBOX_FAILURE_STATE_DRIFT = "STATE_DRIFT"
SANDBOX_FAILURE_UNKNOWN = "UNKNOWN"

SANDBOX_FAILURE_REASON_ALLOWLIST = {
    SANDBOX_FAILURE_NONE,
    SANDBOX_FAILURE_QUERY_ERROR,
    SANDBOX_FAILURE_TIMEOUT,
    SANDBOX_FAILURE_ROLE_SWITCH_FAILURE,
    SANDBOX_FAILURE_RESET_FAILURE,
    SANDBOX_FAILURE_STATE_DRIFT,
    SANDBOX_FAILURE_UNKNOWN,
}


def bound_sandbox_failure_reason(raw_reason: Optional[str]) -> str:
    """Return a bounded sandbox failure reason safe for telemetry."""
    normalized = str(raw_reason or "").strip().upper()
    if normalized in SANDBOX_FAILURE_REASON_ALLOWLIST:
        return normalized
    return SANDBOX_FAILURE_UNKNOWN


def build_postgres_sandbox_metadata(
    *,
    applied: bool,
    rollback: bool,
    failure_reason: str,
) -> dict[str, Any]:
    """Build bounded sandbox metadata shared by spans and envelopes."""
    return {
        "sandbox_applied": bool(applied),
        "sandbox_rollback": bool(rollback),
        "sandbox_failure_reason": bound_sandbox_failure_reason(failure_reason),
    }


class PostgresSandboxStateError(RuntimeError):
    """Raised when strict sandbox state hygiene checks fail."""

    def __init__(self, message: str, *, drift_keys: Optional[list[str]] = None) -> None:
        """Initialize deterministic state hygiene failure details."""
        super().__init__(message)
        self.drift_keys = drift_keys or []


class PostgresSandboxExecutionError(RuntimeError):
    """Raised for deterministic sandbox execution failures."""

    def __init__(self, message: str, *, failure_reason: str) -> None:
        """Initialize deterministic failure metadata for sandbox exceptions."""
        super().__init__(message)
        self.failure_reason = bound_sandbox_failure_reason(failure_reason)


@dataclass(frozen=True)
class PostgresExecutionSandboxResult:
    """Result payload emitted by sandbox exit handling."""

    committed: bool
    rolled_back: bool
    reset_role_attempted: bool
    reset_all_attempted: bool
    state_clean: bool
    failure_reason: str


class PostgresExecutionSandbox:
    """Ensure execution always occurs within a single explicit transaction."""

    def __init__(
        self,
        conn,
        *,
        read_only: bool,
        metadata_sink: Optional[dict[str, Any]] = None,
    ) -> None:
        """Initialize sandbox wrapper around an asyncpg-like connection."""
        self._conn = conn
        self._read_only = read_only
        self._transaction = None
        self._metadata_sink = metadata_sink
        self._baseline_role: Optional[str] = None
        self._baseline_gucs: dict[str, Optional[str]] = {}
        self._drift_keys: list[str] = []
        self.result = PostgresExecutionSandboxResult(
            committed=False,
            rolled_back=False,
            reset_role_attempted=False,
            reset_all_attempted=False,
            state_clean=True,
            failure_reason=SANDBOX_FAILURE_NONE,
        )
        self._update_metadata_sink()

    def _result_metadata(self) -> dict[str, Any]:
        return build_postgres_sandbox_metadata(
            applied=True,
            rollback=self.result.rolled_back,
            failure_reason=self.result.failure_reason,
        )

    def _update_metadata_sink(self) -> None:
        if self._metadata_sink is None:
            return
        self._metadata_sink.clear()
        self._metadata_sink.update(self._result_metadata())

    @staticmethod
    def _attach_metadata_to_exception(
        exc: Optional[BaseException], metadata: dict[str, Any]
    ) -> None:
        if exc is None:
            return
        try:
            setattr(exc, "postgres_sandbox_metadata", dict(metadata))
        except Exception:
            return

    @staticmethod
    def _classify_failure_reason(exc_val: Optional[BaseException]) -> str:
        if exc_val is None:
            return SANDBOX_FAILURE_NONE
        explicit_reason = getattr(exc_val, "failure_reason", None)
        if isinstance(explicit_reason, str):
            bounded_reason = bound_sandbox_failure_reason(explicit_reason)
            if bounded_reason != SANDBOX_FAILURE_UNKNOWN:
                return bounded_reason
        if isinstance(exc_val, PostgresSandboxExecutionError):
            return exc_val.failure_reason
        if isinstance(exc_val, TimeoutError):
            return SANDBOX_FAILURE_TIMEOUT
        return SANDBOX_FAILURE_QUERY_ERROR

    async def _safe_fetchval(self, sql: str) -> Optional[str]:
        fetchval = getattr(self._conn, "fetchval", None)
        if not callable(fetchval):
            return None
        try:
            value = await fetchval(sql)
        except Exception:
            return None
        if value is None:
            return None
        return str(value)

    async def _safe_execute(self, sql: str) -> bool:
        execute = getattr(self._conn, "execute", None)
        if not callable(execute):
            return False
        try:
            await execute(sql)
            return True
        except Exception:
            return False

    async def _capture_baseline_state(self) -> None:
        self._baseline_role = await self._safe_fetchval("SELECT current_setting('role', true)")
        for key in (
            "search_path",
            "statement_timeout",
            "lock_timeout",
            "idle_in_transaction_session_timeout",
        ):
            self._baseline_gucs[key] = await self._safe_fetchval(
                f"SELECT current_setting('{key}', true)"
            )

    async def _validate_clean_state(self) -> tuple[bool, list[str]]:
        drift_keys: list[str] = []

        current_role = await self._safe_fetchval("SELECT current_setting('role', true)")
        if self._baseline_role is not None and current_role != self._baseline_role:
            drift_keys.append("role")

        for key, baseline_value in self._baseline_gucs.items():
            current_value = await self._safe_fetchval(f"SELECT current_setting('{key}', true)")
            if baseline_value is not None and current_value != baseline_value:
                drift_keys.append(key)
        return (len(drift_keys) == 0), drift_keys

    async def __aenter__(self) -> "PostgresExecutionSandbox":
        """Begin explicit transaction scope for sandboxed execution."""
        await self._capture_baseline_state()
        self._transaction = self._conn.transaction(readonly=self._read_only)
        await self._transaction.__aenter__()
        self._update_metadata_sink()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        """Exit sandbox and preserve rollback-on-exception semantics."""
        if self._transaction is None:
            return False

        await self._transaction.__aexit__(exc_type, exc_val, exc_tb)
        reset_role_attempted = await self._safe_execute("RESET ROLE")
        reset_all_attempted = await self._safe_execute("RESET ALL")
        state_clean, drift_keys = await self._validate_clean_state()
        self._drift_keys = drift_keys
        failure_reason = self._classify_failure_reason(exc_val)
        if failure_reason == SANDBOX_FAILURE_NONE:
            if not reset_role_attempted or not reset_all_attempted:
                failure_reason = SANDBOX_FAILURE_RESET_FAILURE
            elif not state_clean:
                failure_reason = SANDBOX_FAILURE_STATE_DRIFT
        self.result = PostgresExecutionSandboxResult(
            committed=exc_type is None,
            rolled_back=exc_type is not None,
            reset_role_attempted=reset_role_attempted,
            reset_all_attempted=reset_all_attempted,
            state_clean=state_clean,
            failure_reason=failure_reason,
        )
        metadata = self._result_metadata()
        self._update_metadata_sink()
        self._attach_metadata_to_exception(exc_val, metadata)
        if drift_keys and bool(get_env_bool("POSTGRES_SANDBOX_STRICT_STATE_CHECK", False)):
            strict_error = PostgresSandboxStateError(
                "Postgres sandbox detected connection state drift after reset.",
                drift_keys=drift_keys,
            )
            self._attach_metadata_to_exception(strict_error, metadata)
            raise strict_error
        return False
