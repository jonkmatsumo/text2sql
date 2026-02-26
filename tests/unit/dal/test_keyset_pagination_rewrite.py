import sqlglot

from dal.keyset_pagination import KeysetOrderKey, apply_keyset_pagination, canonicalize_keyset_sql


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
            explicit_nulls_order=False,
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
            explicit_nulls_order=False,
        ),
        KeysetOrderKey(
            expression=sqlglot.exp.Column(this=sqlglot.exp.Identifier(this="id", quoted=False)),
            alias="id",
            descending=False,
            nulls_first=False,
            explicit_nulls_order=False,
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
            explicit_nulls_order=False,
        )
    ]

    rewritten = apply_keyset_pagination(expression, keys, [10])
    sql_out = rewritten.sql()
    assert "score > 10 OR score IS NULL" in sql_out
    assert "score > NULL" not in sql_out


def test_apply_keyset_pagination_null_cursor_asc_nulls_last_uses_is_null_path():
    """Postgres ASC NULLS LAST with NULL cursor should only advance inside the NULL partition."""
    sql = "SELECT * FROM users ORDER BY score ASC NULLS LAST, id ASC"
    expression = sqlglot.parse_one(sql)
    keys = [
        KeysetOrderKey(
            expression=sqlglot.exp.Column(this=sqlglot.exp.Identifier(this="score", quoted=False)),
            alias="score",
            descending=False,
            nulls_first=False,
            explicit_nulls_order=False,
        ),
        KeysetOrderKey(
            expression=sqlglot.exp.Column(this=sqlglot.exp.Identifier(this="id", quoted=False)),
            alias="id",
            descending=False,
            nulls_first=False,
            explicit_nulls_order=False,
        ),
    ]

    rewritten = apply_keyset_pagination(expression, keys, [None, 50])
    sql_out = rewritten.sql()
    assert "score IS NULL AND (id > 50 OR id IS NULL)" in sql_out
    assert "score = NULL" not in sql_out
    assert "score > NULL" not in sql_out


def test_apply_keyset_pagination_null_cursor_desc_nulls_first_uses_is_null_path():
    """Postgres DESC NULLS FIRST should advance to non-NULL rows and NULL peers."""
    sql = "SELECT * FROM users ORDER BY score DESC NULLS FIRST, id ASC"
    expression = sqlglot.parse_one(sql)
    keys = [
        KeysetOrderKey(
            expression=sqlglot.exp.Column(this=sqlglot.exp.Identifier(this="score", quoted=False)),
            alias="score",
            descending=True,
            nulls_first=True,
            explicit_nulls_order=False,
        ),
        KeysetOrderKey(
            expression=sqlglot.exp.Column(this=sqlglot.exp.Identifier(this="id", quoted=False)),
            alias="id",
            descending=False,
            nulls_first=False,
            explicit_nulls_order=False,
        ),
    ]

    rewritten = apply_keyset_pagination(expression, keys, [None, 50], provider="postgres")
    sql_out = rewritten.sql()
    assert "NOT score IS NULL" in sql_out
    assert "score IS NULL AND (id > 50 OR id IS NULL)" in sql_out
    assert "score = NULL" not in sql_out
    assert "score > NULL" not in sql_out


def test_apply_keyset_pagination_non_postgres_nulls_fail_closed():
    """Non-Postgres providers keep conservative NULL handling."""
    sql = "SELECT * FROM users ORDER BY score ASC NULLS LAST"
    expression = sqlglot.parse_one(sql)
    keys = [
        KeysetOrderKey(
            expression=sqlglot.exp.Column(this=sqlglot.exp.Identifier(this="score", quoted=False)),
            alias="score",
            descending=False,
            nulls_first=False,
            explicit_nulls_order=False,
        )
    ]
    rewritten = apply_keyset_pagination(expression, keys, [10], provider="mysql")
    sql_out = rewritten.sql()
    assert "score > 10" in sql_out
    assert "score IS NULL" not in sql_out


def test_apply_keyset_pagination_sql_output_is_deterministic():
    """Equivalent rewrites should emit identical SQL strings."""
    sql = "SELECT id, score FROM users ORDER BY score DESC NULLS FIRST, id ASC"
    keys = [
        KeysetOrderKey(
            expression=sqlglot.exp.Column(this=sqlglot.exp.Identifier(this="score", quoted=False)),
            alias="score",
            descending=True,
            nulls_first=True,
            explicit_nulls_order=False,
        ),
        KeysetOrderKey(
            expression=sqlglot.exp.Column(this=sqlglot.exp.Identifier(this="id", quoted=False)),
            alias="id",
            descending=False,
            nulls_first=False,
            explicit_nulls_order=False,
        ),
    ]
    first = canonicalize_keyset_sql(
        apply_keyset_pagination(sqlglot.parse_one(sql), keys, [None, 3], provider="postgres"),
        provider="postgres",
    )
    second = canonicalize_keyset_sql(
        apply_keyset_pagination(sqlglot.parse_one(sql), keys, [None, 3], provider="postgres"),
        provider="postgres",
    )
    assert first == second


def test_apply_keyset_pagination_canonical_sql_snapshot():
    """Canonical SQL output should remain stable for snapshot-style assertions."""
    sql = "SELECT id, created_at FROM orders ORDER BY created_at DESC, id ASC"
    expression = sqlglot.parse_one(sql, read="postgres")
    keys = [
        KeysetOrderKey(
            expression=sqlglot.exp.Column(
                this=sqlglot.exp.Identifier(this="created_at", quoted=False)
            ),
            alias="created_at",
            descending=True,
            nulls_first=True,
            explicit_nulls_order=False,
        ),
        KeysetOrderKey(
            expression=sqlglot.exp.Column(this=sqlglot.exp.Identifier(this="id", quoted=False)),
            alias="id",
            descending=False,
            nulls_first=False,
            explicit_nulls_order=False,
        ),
    ]

    rewritten = apply_keyset_pagination(expression, keys, ["2024-01-01", 100], provider="postgres")
    canonical = canonicalize_keyset_sql(rewritten, provider="postgres")
    assert canonical == (
        "SELECT id, created_at FROM orders WHERE created_at < '2024-01-01' OR "
        "(created_at = '2024-01-01' AND (id > 100 OR id IS NULL)) ORDER BY created_at DESC, id ASC"
    )


def test_apply_keyset_pagination_multi_key_predicate_ordering_is_stable():
    """Multi-key predicate clause ordering should remain deterministic."""
    sql = (
        "SELECT id, created_at, priority FROM orders ORDER BY created_at DESC, priority ASC, id ASC"
    )
    keys = [
        KeysetOrderKey(
            expression=sqlglot.exp.Column(
                this=sqlglot.exp.Identifier(this="created_at", quoted=False)
            ),
            alias="created_at",
            descending=True,
            nulls_first=True,
            explicit_nulls_order=False,
        ),
        KeysetOrderKey(
            expression=sqlglot.exp.Column(
                this=sqlglot.exp.Identifier(this="priority", quoted=False)
            ),
            alias="priority",
            descending=False,
            nulls_first=False,
            explicit_nulls_order=False,
        ),
        KeysetOrderKey(
            expression=sqlglot.exp.Column(this=sqlglot.exp.Identifier(this="id", quoted=False)),
            alias="id",
            descending=False,
            nulls_first=False,
            explicit_nulls_order=False,
        ),
    ]
    first = canonicalize_keyset_sql(
        apply_keyset_pagination(
            sqlglot.parse_one(sql, read="postgres"),
            keys,
            ["2024-01-01", 3, 42],
            provider="postgres",
        ),
        provider="postgres",
    )
    second = canonicalize_keyset_sql(
        apply_keyset_pagination(
            sqlglot.parse_one(sql, read="postgres"),
            keys,
            ["2024-01-01", 3, 42],
            provider="postgres",
        ),
        provider="postgres",
    )
    assert first == second
    assert (
        "created_at < '2024-01-01' OR "
        "(created_at = '2024-01-01' AND (priority > 3 OR priority IS NULL OR "
        "(priority = 3 AND (id > 42 OR id IS NULL))))"
    ) in first
