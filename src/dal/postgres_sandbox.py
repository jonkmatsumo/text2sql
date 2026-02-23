"""Postgres transaction sandbox utilities for tool execution."""

from __future__ import annotations

from dataclasses import dataclass
from types import TracebackType
from typing import Optional, Type

from common.config.env import get_env_bool


class PostgresSandboxStateError(RuntimeError):
    """Raised when strict sandbox state hygiene checks fail."""

    def __init__(self, message: str, *, drift_keys: Optional[list[str]] = None) -> None:
        """Initialize deterministic state hygiene failure details."""
        super().__init__(message)
        self.drift_keys = drift_keys or []


@dataclass(frozen=True)
class PostgresExecutionSandboxResult:
    """Result payload emitted by sandbox exit handling."""

    committed: bool
    rolled_back: bool
    reset_role_attempted: bool
    reset_all_attempted: bool
    state_clean: bool


class PostgresExecutionSandbox:
    """Ensure execution always occurs within a single explicit transaction."""

    def __init__(self, conn, *, read_only: bool) -> None:
        """Initialize sandbox wrapper around an asyncpg-like connection."""
        self._conn = conn
        self._read_only = read_only
        self._transaction = None
        self._baseline_role: Optional[str] = None
        self._baseline_gucs: dict[str, Optional[str]] = {}
        self._drift_keys: list[str] = []
        self.result = PostgresExecutionSandboxResult(
            committed=False,
            rolled_back=False,
            reset_role_attempted=False,
            reset_all_attempted=False,
            state_clean=True,
        )

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
        self.result = PostgresExecutionSandboxResult(
            committed=exc_type is None,
            rolled_back=exc_type is not None,
            reset_role_attempted=reset_role_attempted,
            reset_all_attempted=reset_all_attempted,
            state_clean=state_clean,
        )
        if drift_keys and bool(get_env_bool("POSTGRES_SANDBOX_STRICT_STATE_CHECK", False)):
            raise PostgresSandboxStateError(
                "Postgres sandbox detected connection state drift after reset.",
                drift_keys=drift_keys,
            )
        return False
