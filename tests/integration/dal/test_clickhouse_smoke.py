import os
import shutil
import subprocess
import time

import pytest


@pytest.mark.integration
def test_clickhouse_docker_smoke():
    """Optional ClickHouse docker smoke test."""
    if os.environ.get("CLICKHOUSE_SMOKE") != "1":
        pytest.skip("CLICKHOUSE_SMOKE not set")
    if not shutil.which("docker"):
        pytest.skip("docker not available")

    container_name = "text2sql-clickhouse-smoke"
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            container_name,
            "-p",
            "9000:9000",
            "clickhouse/clickhouse-server:latest",
        ],
        check=True,
    )
    try:
        time.sleep(5)
    finally:
        subprocess.run(["docker", "stop", container_name], check=False)
