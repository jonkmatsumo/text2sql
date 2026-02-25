import sqlglot

from dal.keyset_pagination import KeysetOrderKey, apply_keyset_pagination


def test_apply_keyset_pagination_single_column():
    """Test SQL rewrite for a single column keyset."""
    sql = "SELECT * FROM users ORDER BY id ASC"
    expression = sqlglot.parse_one(sql)
    order_keys = [
        KeysetOrderKey(
            expression=sqlglot.exp.Column(this=sqlglot.exp.Identifier(this="id", quoted=False)),
            alias="id",
            descending=False,
            nulls_first=False,
        )
    ]
    values = [50]

    rewritten = apply_keyset_pagination(expression, order_keys, values)
    assert "WHERE id > 50" in rewritten.sql()


def test_apply_keyset_pagination_multi_column():
    """Test SQL rewrite for multi-column keyset predicates."""
    sql = "SELECT id, created_at FROM orders ORDER BY created_at DESC, id ASC"
    expression = sqlglot.parse_one(sql)
    keys = [
        KeysetOrderKey(
            expression=sqlglot.exp.Column(
                this=sqlglot.exp.Identifier(this="created_at", quoted=False)
            ),
            alias="created_at",
            descending=True,
            nulls_first=True,
        ),
        KeysetOrderKey(
            expression=sqlglot.exp.Column(this=sqlglot.exp.Identifier(this="id", quoted=False)),
            alias="id",
            descending=False,
            nulls_first=False,
        ),
    ]
    vals = ["2024-01-01", 100]

    rewritten = apply_keyset_pagination(expression, keys, vals)
    # expected: (created_at < '2024-01-01') OR (created_at = '2024-01-01' AND id > 100)
    sql_out = rewritten.sql()
    assert "created_at < '2024-01-01'" in sql_out
    assert "created_at = '2024-01-01' AND (id > 100 OR id IS NULL)" in sql_out


def test_apply_keyset_pagination_nulls_last_includes_null_rows():
    """Rows with NULL values should remain reachable for NULLS LAST ordering."""
    sql = "SELECT * FROM users ORDER BY score ASC NULLS LAST"
    expression = sqlglot.parse_one(sql)
    keys = [
        KeysetOrderKey(
            expression=sqlglot.exp.Column(this=sqlglot.exp.Identifier(this="score", quoted=False)),
            alias="score",
            descending=False,
            nulls_first=False,
        )
    ]

    rewritten = apply_keyset_pagination(expression, keys, [10])
    sql_out = rewritten.sql()
    assert "score > 10 OR score IS NULL" in sql_out
    assert "score > NULL" not in sql_out


def test_apply_keyset_pagination_null_cursor_uses_is_null_path():
    """NULL cursor values should use IS NULL semantics instead of equality/comparison to NULL."""
    sql = "SELECT * FROM users ORDER BY score DESC NULLS FIRST, id ASC"
    expression = sqlglot.parse_one(sql)
    keys = [
        KeysetOrderKey(
            expression=sqlglot.exp.Column(this=sqlglot.exp.Identifier(this="score", quoted=False)),
            alias="score",
            descending=True,
            nulls_first=True,
        ),
        KeysetOrderKey(
            expression=sqlglot.exp.Column(this=sqlglot.exp.Identifier(this="id", quoted=False)),
            alias="id",
            descending=False,
            nulls_first=False,
        ),
    ]

    rewritten = apply_keyset_pagination(expression, keys, [None, 50])
    sql_out = rewritten.sql()
    assert "NOT score IS NULL" in sql_out
    assert "score IS NULL AND (id > 50 OR id IS NULL)" in sql_out
    assert "score = NULL" not in sql_out
    assert "score > NULL" not in sql_out
