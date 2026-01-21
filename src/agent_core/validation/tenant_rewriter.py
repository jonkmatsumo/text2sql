"""Tenant Rewriter for AST-based RLS simulation.

Rewrites SQL queries to inject tenant isolation predicates dynamically.
"""

import logging
from typing import Dict

import sqlglot
from sqlglot import exp

from agent_core.validation.policy_loader import PolicyDefinition, PolicyLoader

logger = logging.getLogger(__name__)


class TenantRewriter:
    """Rewrite SQL to enforce tenant isolation."""

    @classmethod
    async def rewrite_sql(cls, sql: str, tenant_id: int) -> str:
        """Rewrite SQL to inject tenant predicates.

        Args:
            sql: Original SQL string.
            tenant_id: The tenant ID to bind.

        Returns:
            Rewritten SQL string with :tenant_id placeholders.
        """
        # 1. Load policies
        policies = await PolicyLoader.get_policies()
        if not policies:
            logger.warning("No policies loaded. Returning original SQL.")
            return sql

        # 2. Parse SQL
        try:
            expression = sqlglot.parse_one(sql)
        except Exception as e:
            raise ValueError(f"Failed to parse SQL for rewriting: {e}")

        # 3. Apply rewriting transformation
        # We process the tree to find Table nodes and replace them with filtered subqueries
        # or inject WHERE clauses.
        # Strategy: Replace 'Table' with '(SELECT * FROM Table WHERE tenant_col = :tenant_id)'
        # This preserves JOIN semantics (e.g. OUTER JOINs) better than appending to global WHERE.

        rewritten = cls._rewrite_node(expression, policies)

        # 4. Generate SQL
        # We output using default postgres dialect
        return rewritten.sql(dialect="postgres")

    @classmethod
    def _rewrite_node(
        cls, node: exp.Expression, policies: Dict[str, PolicyDefinition]
    ) -> exp.Expression:
        """Recursively rewrite nodes."""  # noqa: D202

        # Walk and transform. We use transform because it handles replacement in-place
        # or returns new node.
        def transformer(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.Table):
                table_name = node.name.lower()
                if table_name in policies:
                    policy = policies[table_name]
                    return cls._apply_policy_to_table(node, policy)
            return node

        return node.transform(transformer)

    @classmethod
    def _apply_policy_to_table(
        cls, table_node: exp.Table, policy: PolicyDefinition
    ) -> exp.Expression:
        """Replace a Table definition with a filtered Subquery."""
        # Original:  FROM table AS alias
        # New:       FROM (SELECT * FROM table WHERE filtered) AS alias

        alias = table_node.alias or table_node.name

        # Generate the inner subquery
        # SELECT * FROM table WHERE store_id = :tenant_id

        # Note: We use a placeholder :tenant_id. The executor will bind the actual value.
        # We rely on sqlglot to generate the parameter syntax.

        # Check if we are already inside a wrapper we created?
        # No, the transformer visits top-down or bottom-up.
        # Defaults to bottom-up. So we replace the leaf Table.

        # Construct the inner SELECT
        inner_select = exp.select("*").from_(table_node.this)

        # Add WHERE clause
        # using sqlglot expression builder
        # Condition: tenant_column = :tenant_id

        # We need to ensure we don't recurse infinitely if we parse within the transformer?
        # Transform replaces the node.

        col_ident = exp.Identifier(this=policy.tenant_column, quoted=False)
        # Use a Parameter node for :tenant_id
        # In sqlglot, :name is typically a Placeholder or Parameter
        param = exp.Parameter(this=exp.Var(this="tenant_id"))

        condition = exp.EQ(this=exp.Column(this=col_ident), expression=param)

        inner_select = inner_select.where(condition)

        # Wrap in subquery
        subquery = inner_select.subquery(alias=alias)

        return subquery
