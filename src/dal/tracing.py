import hashlib
from typing import Any, Awaitable, Dict, Optional

from common.config.env import get_env_bool


def trace_enabled() -> bool:
    """Return True when DAL query tracing is enabled."""
    return get_env_bool("DAL_TRACE_QUERIES", False)


def _hash_sql(sql: str) -> str:
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


async def trace_query_operation(
    name: str,
    provider: str,
    execution_model: str,
    sql: Optional[str],
    operation: Awaitable,
):
    """Trace a DAL query operation with OTEL when enabled."""
    if not trace_enabled():
        return await operation

    from opentelemetry import trace

    tracer = trace.get_tracer("dal")
    with tracer.start_as_current_span(name) as span:
        span.set_attribute("db.provider", provider)
        span.set_attribute("db.execution_model", execution_model)
        if sql:
            span.set_attribute("db.statement_hash", _hash_sql(sql))
        try:
            result = await operation
            span.set_attribute("db.status", "ok")
            return result
        except Exception:
            span.set_attribute("db.status", "error")
            raise


class TracedAsyncpgConnection:
    """Proxy for asyncpg connections that emits query tracing spans."""

    def __init__(self, conn: Any, provider: str, execution_model: str) -> None:
        """Initialize the traced connection wrapper."""
        self._conn = conn
        self._provider = provider
        self._execution_model = execution_model

    async def execute(self, sql: str, *params: Any) -> str:
        """Execute a statement with tracing when enabled."""

        async def _run():
            return await self._conn.execute(sql, *params)

        return await trace_query_operation(
            "dal.query.execute",
            provider=self._provider,
            execution_model=self._execution_model,
            sql=sql,
            operation=_run(),
        )

    async def fetch(self, sql: str, *params: Any) -> list[Dict[str, Any]]:
        """Fetch rows with tracing when enabled."""

        async def _run():
            rows = await self._conn.fetch(sql, *params)
            return [dict(row) for row in rows]

        return await trace_query_operation(
            "dal.query.execute",
            provider=self._provider,
            execution_model=self._execution_model,
            sql=sql,
            operation=_run(),
        )

    async def fetchrow(self, sql: str, *params: Any) -> Optional[Dict[str, Any]]:
        """Fetch a single row with tracing when enabled."""
        rows = await self.fetch(sql, *params)
        return rows[0] if rows else None

    async def fetchval(self, sql: str, *params: Any) -> Any:
        """Fetch a single scalar value with tracing when enabled."""
        row = await self.fetchrow(sql, *params)
        if row is None:
            return None
        return next(iter(row.values()))
