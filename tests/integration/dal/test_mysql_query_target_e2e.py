import asyncio
import json
import os
import shutil
import socket
import subprocess
import uuid

import aiomysql
import pytest

from dal.database import Database
from mcp_server.tools.execute_sql_query import handler as execute_sql_query


def _get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("", 0))
        return sock.getsockname()[1]


async def _wait_for_mysql(host: str, port: int, user: str, password: str, db: str) -> None:
    for _ in range(30):
        try:
            conn = await aiomysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                db=db,
                autocommit=True,
            )
            conn.close()
            return
        except Exception:
            await asyncio.sleep(1)
    raise RuntimeError("Timed out waiting for MySQL to be ready.")


@pytest.mark.asyncio
async def test_mysql_query_target_introspection_and_exec():
    """Exercise MySQL introspection + parameterized execution via execute_sql_query."""
    if not shutil.which("docker"):
        pytest.skip("docker not available")

    container_name = f"text2sql-mysql-{uuid.uuid4().hex[:8]}"
    port = _get_free_port()
    env = os.environ.copy()
    env.update(
        {
            "QUERY_TARGET_PROVIDER": "mysql",
            "DB_HOST": "127.0.0.1",
            "DB_PORT": str(port),
            "DB_NAME": "query_target",
            "DB_USER": "root",
            "DB_PASS": "secret",
        }
    )

    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            container_name,
            "-e",
            "MYSQL_ROOT_PASSWORD=secret",
            "-e",
            "MYSQL_DATABASE=query_target",
            "-p",
            f"{port}:3306",
            "mysql:8.0",
        ],
        check=True,
    )

    try:
        await _wait_for_mysql(
            host=env["DB_HOST"],
            port=int(env["DB_PORT"]),
            user=env["DB_USER"],
            password=env["DB_PASS"],
            db=env["DB_NAME"],
        )

        conn = await aiomysql.connect(
            host=env["DB_HOST"],
            port=int(env["DB_PORT"]),
            user=env["DB_USER"],
            password=env["DB_PASS"],
            db=env["DB_NAME"],
            autocommit=True,
        )
        async with conn.cursor() as cursor:
            await cursor.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))")
            await cursor.execute("INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 'Bob')")
        conn.close()

        os.environ.update(env)
        await Database.init()
        try:
            introspector = Database.get_schema_introspector()
            table_names = await introspector.list_table_names()
            assert table_names == ["users"]

            result_json = await execute_sql_query(
                'SELECT "name" FROM "users" WHERE id = $1', tenant_id=1, params=[1]
            )
            assert json.loads(result_json) == [{"name": "Ada"}]
        finally:
            await Database.close()
    finally:
        subprocess.run(["docker", "stop", container_name], check=False)
