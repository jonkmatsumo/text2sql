import hashlib
import inspect
from typing import Any, Awaitable, Dict, Optional

from common.observability.context import run_id_var
from common.observability.metrics import is_metrics_enabled
from dal.util.read_only import enforce_read_only_sql
from dal.util.row_limits import cap_rows_with_metadata


def trace_enabled() -> bool:
    """Return True when DAL query tracing is enabled or OTEL exporter defaults apply."""
    return is_metrics_enabled("DAL_TRACE_QUERIES")


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
        run_id = run_id_var.get()
        if run_id:
            span.set_attribute("run_id", run_id)
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

    def __init__(
        self,
        conn: Any,
        provider: str,
        execution_model: str,
        max_rows: int = 0,
        read_only: bool = False,
        session_guardrail_metadata: Optional[Dict[str, Any]] = None,
        postgres_sandbox_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Initialize the traced connection wrapper."""
        self._conn = conn
        self._provider = provider
        self._execution_model = execution_model
        self._max_rows = max_rows
        self._read_only = read_only
        self._session_guardrail_metadata = session_guardrail_metadata or {}
        self._postgres_sandbox_metadata = postgres_sandbox_metadata or {}
        self._last_truncated = False
        self._last_truncated_reason: Optional[str] = None

    @property
    def last_truncated(self) -> bool:
        """Return True when the last fetch was truncated by row limits."""
        return self._last_truncated

    @property
    def last_truncated_reason(self) -> Optional[str]:
        """Return the reason when the last fetch was truncated."""
        return self._last_truncated_reason

    @property
    def session_guardrail_metadata(self) -> Dict[str, Any]:
        """Return bounded session guardrail metadata attached by the DAL."""
        return dict(self._session_guardrail_metadata)

    @property
    def postgres_sandbox_metadata(self) -> Dict[str, Any]:
        """Return bounded sandbox metadata attached by the DAL."""
        return dict(self._postgres_sandbox_metadata)

    async def execute(self, sql: str, *params: Any) -> str:
        """Execute a statement with tracing when enabled."""
        enforce_read_only_sql(sql, self._provider, self._read_only)

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
        enforce_read_only_sql(sql, self._provider, self._read_only)

        async def _run():
            rows = await self._conn.fetch(sql, *params)
            capped_rows, truncated = cap_rows_with_metadata(
                [dict(row) for row in rows], self._max_rows
            )
            self._last_truncated = truncated
            self._last_truncated_reason = "PROVIDER_CAP" if truncated else None
            return capped_rows

        return await trace_query_operation(
            "dal.query.execute",
            provider=self._provider,
            execution_model=self._execution_model,
            sql=sql,
            operation=_run(),
        )

    async def fetch_with_columns(self, sql: str, *params: Any) -> tuple[list[Dict[str, Any]], list]:
        """Fetch rows with column metadata when supported."""
        enforce_read_only_sql(sql, self._provider, self._read_only)

        async def _run():
            from dal.util.column_metadata import columns_from_asyncpg_attributes

            statement = await self._conn.prepare(sql)
            attrs = statement.get_attributes()
            rows = await statement.fetch(*params)
            capped_rows, truncated = cap_rows_with_metadata(
                [dict(row) for row in rows], self._max_rows
            )
            self._last_truncated = truncated
            self._last_truncated_reason = "PROVIDER_CAP" if truncated else None
            return capped_rows, columns_from_asyncpg_attributes(attrs)

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

    async def cancel(self) -> None:
        """Best-effort cancellation for in-flight queries."""
        cancel_fn = getattr(self._conn, "cancel", None)
        if not callable(cancel_fn):
            return
        result = cancel_fn()
        if inspect.isawaitable(result):
            await result

    async def fetchval(self, sql: str, *params: Any) -> Any:
        """Fetch a single scalar value with tracing when enabled."""
        row = await self.fetchrow(sql, *params)
        if row is None:
            return None
        return next(iter(row.values()))
