"""Postgres transaction sandbox utilities for tool execution."""

from __future__ import annotations

from dataclasses import dataclass
from types import TracebackType
from typing import Optional, Type


@dataclass(frozen=True)
class PostgresExecutionSandboxResult:
    """Result payload emitted by sandbox exit handling."""

    committed: bool
    rolled_back: bool


class PostgresExecutionSandbox:
    """Ensure execution always occurs within a single explicit transaction."""

    def __init__(self, conn, *, read_only: bool) -> None:
        """Initialize sandbox wrapper around an asyncpg-like connection."""
        self._conn = conn
        self._read_only = read_only
        self._transaction = None
        self.result = PostgresExecutionSandboxResult(committed=False, rolled_back=False)

    async def __aenter__(self) -> "PostgresExecutionSandbox":
        """Begin explicit transaction scope for sandboxed execution."""
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
        self.result = PostgresExecutionSandboxResult(
            committed=exc_type is None,
            rolled_back=exc_type is not None,
        )
        return False
