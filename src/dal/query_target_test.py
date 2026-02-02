import asyncio
import os
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class QueryTargetTestResult:
    """Result for query-target test-connection."""

    ok: bool
    error_code: Optional[str] = None
    error_message: Optional[str] = None


class QueryTargetTestError(RuntimeError):
    """Raised for query-target test errors."""

    def __init__(self, code: str, message: str) -> None:
        """Initialize error with code and message."""
        super().__init__(message)
        self.code = code
        self.message = message


async def test_query_target_connection(
    provider: str,
    metadata: Dict[str, Any],
    auth: Dict[str, Any],
    guardrails: Dict[str, Any],
) -> QueryTargetTestResult:
    """Run a minimal connection probe for a query-target config."""
    _ = guardrails
    try:
        if provider in {"postgres", "cockroachdb"}:
            await _test_postgres_like(metadata, auth, default_port=5432)
        elif provider == "redshift":
            await _test_postgres_like(metadata, auth, default_port=5439)
        elif provider == "mysql":
            await _test_mysql(metadata, auth)
        elif provider == "sqlite":
            await _test_sqlite(metadata)
        elif provider == "duckdb":
            await _test_duckdb(metadata, guardrails)
        elif provider == "clickhouse":
            await _test_clickhouse(metadata, auth)
        elif provider == "snowflake":
            await _test_snowflake(metadata, auth)
        elif provider == "bigquery":
            await _test_bigquery(metadata)
        elif provider == "athena":
            await _test_athena(metadata)
        elif provider == "databricks":
            await _test_databricks(metadata, auth)
        else:
            raise QueryTargetTestError("unsupported_provider", f"Unsupported provider: {provider}")
        return QueryTargetTestResult(ok=True)
    except QueryTargetTestError as exc:
        return QueryTargetTestResult(ok=False, error_code=exc.code, error_message=exc.message)
    except Exception as exc:  # pragma: no cover - defensive
        return QueryTargetTestResult(
            ok=False, error_code="connection_error", error_message=str(exc)
        )


def _resolve_secret_ref(auth: Dict[str, Any], fallback_env: str) -> Optional[str]:
    secret_ref = auth.get("secret_ref")
    if secret_ref and isinstance(secret_ref, str) and secret_ref.startswith("env:"):
        return os.getenv(secret_ref[len("env:") :])
    if secret_ref:
        return None
    return os.getenv(fallback_env)


async def _test_postgres_like(metadata: Dict[str, Any], auth: Dict[str, Any], default_port: int):
    import asyncpg

    password = _resolve_secret_ref(auth, "DB_PASS")
    if not password:
        raise QueryTargetTestError("missing_secret", "Missing database password reference.")
    host = metadata["host"]
    port = int(metadata.get("port") or default_port)
    db_name = metadata["db_name"]
    user = metadata["user"]
    conn = await asyncpg.connect(
        host=host, port=port, user=user, password=password, database=db_name
    )
    try:
        await conn.fetchval("SELECT 1")
    finally:
        await conn.close()


async def _test_mysql(metadata: Dict[str, Any], auth: Dict[str, Any]):
    import aiomysql

    password = _resolve_secret_ref(auth, "DB_PASS") or ""
    conn = await aiomysql.connect(
        host=metadata["host"],
        port=int(metadata.get("port") or 3306),
        user=metadata["user"],
        password=password,
        db=metadata["db_name"],
        autocommit=True,
    )
    try:
        async with conn.cursor() as cursor:
            await cursor.execute("SELECT 1")
            await cursor.fetchone()
    finally:
        conn.close()


async def _test_sqlite(metadata: Dict[str, Any]):
    import aiosqlite

    db_path = metadata["path"]
    conn = await aiosqlite.connect(db_path)
    try:
        await conn.execute("SELECT 1")
    finally:
        await conn.close()


async def _test_duckdb(metadata: Dict[str, Any], guardrails: Dict[str, Any]):
    import duckdb

    read_only = bool(guardrails.get("read_only", False))
    conn = await asyncio.to_thread(duckdb.connect, metadata["path"], read_only=read_only)
    try:
        await asyncio.to_thread(conn.execute, "SELECT 1")
    finally:
        await asyncio.to_thread(conn.close)


async def _test_clickhouse(metadata: Dict[str, Any], auth: Dict[str, Any]):
    from asynch import connect

    password = _resolve_secret_ref(auth, "CLICKHOUSE_PASS") or ""
    conn = await connect(
        host=metadata["host"],
        port=int(metadata.get("port") or 9000),
        database=metadata["database"],
        user=metadata.get("user") or "default",
        password=password,
        secure=bool(metadata.get("secure", False)),
    )
    try:
        await conn.fetch("SELECT 1")
    finally:
        await conn.close()


async def _test_snowflake(metadata: Dict[str, Any], auth: Dict[str, Any]):
    import snowflake.connector

    password = _resolve_secret_ref(auth, "SNOWFLAKE_PASSWORD")
    conn = snowflake.connector.connect(
        account=metadata["account"],
        user=metadata["user"],
        password=password,
        warehouse=metadata["warehouse"],
        database=metadata["database"],
        schema=metadata["schema"],
        role=metadata.get("role"),
        authenticator=metadata.get("authenticator"),
    )
    try:
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1")
            cur.fetchone()
        finally:
            cur.close()
    finally:
        conn.close()


async def _test_bigquery(metadata: Dict[str, Any]):
    from google.cloud import bigquery

    def _run():
        client = bigquery.Client(project=metadata["project"])
        job = client.query("SELECT 1")
        return list(job.result())

    await asyncio.to_thread(_run)


async def _test_athena(metadata: Dict[str, Any]):
    import boto3

    client = boto3.client("athena", region_name=metadata["region"])
    response = client.start_query_execution(
        QueryString="SELECT 1",
        QueryExecutionContext={"Database": metadata["database"]},
        WorkGroup=metadata["workgroup"],
        ResultConfiguration={"OutputLocation": metadata["output_location"]},
    )
    if not response.get("QueryExecutionId"):
        raise QueryTargetTestError("athena_start_failed", "Failed to start Athena query.")


async def _test_databricks(metadata: Dict[str, Any], auth: Dict[str, Any]):
    from dal.databricks.executor import _request

    token = _resolve_secret_ref(auth, "DATABRICKS_TOKEN")
    if not token:
        raise QueryTargetTestError("missing_secret", "Missing Databricks token reference.")
    host = metadata["host"].rstrip("/")
    payload = {
        "statement": "SELECT 1",
        "warehouse_id": metadata["warehouse_id"],
        "catalog": metadata["catalog"],
        "schema": metadata["schema"],
    }
    response = await asyncio.to_thread(
        _request, "POST", f"{host}/api/2.0/sql/statements", token, payload, 30
    )
    statement_id = response.get("statement_id")
    if not statement_id:
        raise QueryTargetTestError("databricks_submit_failed", "Failed to submit statement.")
