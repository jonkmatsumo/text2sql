import pytest

from dal.redshift.validation import validate_redshift_query


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT ARRAY[1,2,3]",
        "SELECT ARRAY(1,2,3)",
        "SELECT 1 WHERE 1 = ANY(ARRAY[1,2])",
        "SELECT '{}'::int[]",
    ],
)
def test_redshift_validation_blocks_arrays(sql):
    """Detect array syntax as unsupported in Redshift."""
    errors = validate_redshift_query(sql)
    assert any("ARRAY" in err or "array" in err for err in errors)


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT payload::jsonb",
        "SELECT data->'key' FROM t",
        "SELECT data->>'key' FROM t",
        "SELECT jsonb_set(data, '{a}', '1')",
    ],
)
def test_redshift_validation_blocks_json_ops(sql):
    """Detect JSONB operators/functions as unsupported in Redshift."""
    errors = validate_redshift_query(sql)
    assert any("JSONB" in err or "jsonb" in err for err in errors)


def test_redshift_validation_allows_simple_select():
    """Allow simple queries without unsupported syntax."""
    errors = validate_redshift_query("SELECT id, name FROM users WHERE id = 1")
    assert errors == []
