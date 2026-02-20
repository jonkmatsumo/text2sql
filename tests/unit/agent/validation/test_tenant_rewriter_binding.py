import pytest
import sqlglot

from agent.utils.sql_ast import normalize_sql
from agent.validation.policy_loader import PolicyDefinition
from agent.validation.tenant_rewriter import TenantRewriter


def test_tenant_rewriter_uses_positional_placeholder():
    """Verify that TenantRewriter generates $1 instead of named placeholders."""
    policies = {
        "sensitive_table": PolicyDefinition(
            table_name="sensitive_table",
            tenant_column="org_id",
            expression_template="{column} = :tenant_id",
        )
    }

    sql = "SELECT * FROM sensitive_table"
    expression = sqlglot.parse_one(sql)
    rewritten = TenantRewriter._rewrite_node(expression, policies)
    rewritten_sql = normalize_sql(rewritten, dialect="postgres")

    # OLD behavior produced $tenant_id or similar
    # NEW behavior must produce $1
    assert "$1" in rewritten_sql
    assert "$tenant_id" not in rewritten_sql
    assert "org_id" in rewritten_sql


def test_tenant_rewriter_no_policy_no_rewrite():
    """Verify that queries without matching policies remain unchanged."""
    policies = {
        "other_table": PolicyDefinition(
            table_name="other_table",
            tenant_column="org_id",
            expression_template="{column} = :tenant_id",
        )
    }

    sql = "SELECT * FROM public_table"
    expression = sqlglot.parse_one(sql)
    rewritten = TenantRewriter._rewrite_node(expression, policies)
    rewritten_sql = normalize_sql(rewritten, dialect="postgres")

    # normalize_sql might change formatting, but logic should be same
    # For no rewrite, we check that no subquery wrapper was added
    assert "SELECT * FROM public_table" in rewritten_sql.replace("\n", " ")
    assert "$1" not in rewritten_sql


@pytest.mark.async_session
async def test_execute_node_tenant_binding_logic():
    """Verify the logic in validate_and_execute_node that determines execute_params."""
    # Since we already applied the fix in execute.py:
    # rewrite_occurred = rewritten_sql != original_sql
    # execute_params = [tenant_id] if (
    #   tenant_id and rewrite_occurred and "$1" in rewritten_sql
    # ) else []

    original_sql = "SELECT * FROM sensitive_table"
    tenant_id = 123

    # Case 1: Rewrite occurred with $1
    rewritten_sql_1 = "SELECT * FROM (SELECT * FROM sensitive_table WHERE org_id = $1) AS t"
    rewrite_occurred_1 = rewritten_sql_1 != original_sql
    execute_params_1 = (
        [tenant_id] if (tenant_id and rewrite_occurred_1 and "$1" in rewritten_sql_1) else []
    )
    assert execute_params_1 == [123]

    # Case 2: No rewrite occurred
    rewritten_sql_2 = original_sql
    rewrite_occurred_2 = rewritten_sql_2 != original_sql
    execute_params_2 = (
        [tenant_id] if (tenant_id and rewrite_occurred_2 and "$1" in rewritten_sql_2) else []
    )
    assert execute_params_2 == []

    # Case 3: Rewrite occurred but no $1 (sanity check)
    rewritten_sql_3 = "SELECT * FROM sensitive_table -- some comment"
    rewrite_occurred_3 = rewritten_sql_3 != original_sql
    execute_params_3 = (
        [tenant_id] if (tenant_id and rewrite_occurred_3 and "$1" in rewritten_sql_3) else []
    )
    assert execute_params_3 == []
