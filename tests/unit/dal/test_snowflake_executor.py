"""Unit tests for Snowflake executor fetch behavior."""

import sys

import pytest

pytest.importorskip("snowflake")

if sys.version_info < (3, 10):
    pytest.skip("Snowflake executor typing requires Python 3.10+", allow_module_level=True)

from dal.snowflake.executor import _fetch  # noqa: E402


class _FakeCursor:
    def __init__(self, batches, description):
        self._batches = list(batches)
        self.description = description
        self.fetchmany_calls = 0

    def fetchmany(self, size):
        _ = size
        self.fetchmany_calls += 1
        if not self._batches:
            return []
        return self._batches.pop(0)


class _FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor

    def get_results_from_sfqid(self, job_id):
        _ = job_id
        return self._cursor


def test_snowflake_fetch_limits_rows():
    """Ensure fetch respects max_rows without fetching all rows."""
    cursor = _FakeCursor(
        batches=[[(1,), (2,)], [(3,), (4,)]],
        description=[("id", None)],
    )
    conn = _FakeConn(cursor)

    rows = _fetch(conn, "job-1", max_rows=3)

    assert rows == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert cursor.fetchmany_calls == 2


def test_snowflake_fetch_all_rows_when_unbounded():
    """Ensure fetch returns all rows when max_rows is None."""
    cursor = _FakeCursor(
        batches=[[(1,), (2,)], [(3,)]],
        description=[("id", None)],
    )
    conn = _FakeConn(cursor)

    rows = _fetch(conn, "job-1", max_rows=None)

    assert rows == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert cursor.fetchmany_calls == 3
