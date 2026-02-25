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
    assert "created_at = '2024-01-01' AND id > 100" in sql_out
