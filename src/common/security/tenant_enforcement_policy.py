from dataclasses import dataclass
from enum import Enum
from typing import Literal, Optional


class TenantSQLShape(Enum):
    """Classification of SQL shapes for tenant enforcement capability."""

    SAFE_SIMPLE_SELECT = "SAFE_SIMPLE_SELECT"
    SAFE_CTE_QUERY = "SAFE_CTE_QUERY"
    UNSUPPORTED_CTE = "UNSUPPORTED_CTE"
    UNSUPPORTED_SUBQUERY = "UNSUPPORTED_SUBQUERY"
    UNSUPPORTED_CORRELATED_SUBQUERY = "UNSUPPORTED_CORRELATED_SUBQUERY"
    UNSUPPORTED_NESTED_FROM = "UNSUPPORTED_NESTED_FROM"
    UNSUPPORTED_SET_OPERATION = "UNSUPPORTED_SET_OPERATION"
    UNSUPPORTED_WINDOW_FUNCTION = "UNSUPPORTED_WINDOW_FUNCTION"
    UNSUPPORTED_STATEMENT_TYPE = "UNSUPPORTED_STATEMENT_TYPE"
    UNSUPPORTED_COMPLEXITY = "UNSUPPORTED_COMPLEXITY"
    PARSE_ERROR = "PARSE_ERROR"


@dataclass(frozen=True)
class TenantEnforcementResult:
    """Result payload mapping enforcement outcomes."""

    applied: bool
    mode: str
    outcome: str
    reason_code: Optional[str]


@dataclass(frozen=True)
class TenantEnforcementPolicy:
    """Policy for determining tenant enforcement rules and outcomes."""

    provider: str
    mode: Literal["sql_rewrite", "rls_session", "none"]
    strict: bool
    max_targets: int
    max_params: int
    max_ast_nodes: int
    hard_timeout_ms: int

    def decide_enforcement(self) -> bool:
        """Evaluate if enforcement should occur through SQL rewrite."""
        return self.mode == "sql_rewrite"

    def classify_sql(self, sql: str, provider: str = "sqlite") -> TenantSQLShape:
        """Classify the provided SQL shape for enforcement capability."""
        import sqlglot
        from sqlglot import exp

        from common.sql.dialect import normalize_sqlglot_dialect

        dialect = normalize_sqlglot_dialect(provider.strip().lower())
        try:
            expressions = sqlglot.parse(sql, read=dialect)
        except Exception:
            return TenantSQLShape.PARSE_ERROR

        if not expressions or len(expressions) != 1 or expressions[0] is None:
            return TenantSQLShape.UNSUPPORTED_STATEMENT_TYPE

        expression = expressions[0]

        # We need to compute nodes here to catch complexity before we analyze
        ast_node_count = sum(1 for _ in expression.walk())
        if ast_node_count > self.max_ast_nodes:
            return TenantSQLShape.UNSUPPORTED_COMPLEXITY

        # Basic shape checks (previously in _assert_rewrite_eligible)
        from common.sql.tenant_sql_rewriter import (
            CTEClassification,
            SubqueryClassification,
            _contains_set_operation,
            _has_correlated_subquery,
            _has_nested_from_subquery,
            classify_cte_query,
            classify_subquery,
        )

        if _contains_set_operation(expression):
            return TenantSQLShape.UNSUPPORTED_SET_OPERATION

        if not isinstance(expression, exp.Select):
            return TenantSQLShape.UNSUPPORTED_STATEMENT_TYPE

        has_cte_args = expression.args.get("with_") is not None
        if has_cte_args:
            if classify_cte_query(expression) == CTEClassification.UNSUPPORTED_CTE:
                return TenantSQLShape.UNSUPPORTED_CTE

        if any(True for _ in expression.find_all(exp.Window)):
            return TenantSQLShape.UNSUPPORTED_WINDOW_FUNCTION

        if _has_nested_from_subquery(expression):
            return TenantSQLShape.UNSUPPORTED_NESTED_FROM

        if _has_correlated_subquery(expression, strict_mode=self.strict):
            return TenantSQLShape.UNSUPPORTED_CORRELATED_SUBQUERY

        with_ = expression.args.get("with_")
        for select in expression.find_all(exp.Select):
            if select is expression:
                continue

            is_cte_body = False
            if with_:
                for cte in with_.expressions:
                    if select is cte.this:
                        is_cte_body = True
                        break
            if is_cte_body:
                continue

            if classify_subquery(select) == SubqueryClassification.UNSUPPORTED_SUBQUERY:
                return TenantSQLShape.UNSUPPORTED_SUBQUERY

        if has_cte_args:
            return TenantSQLShape.SAFE_CTE_QUERY

        return TenantSQLShape.SAFE_SIMPLE_SELECT

    def determine_outcome(
        self,
        *,
        applied: bool,
        reason_code: Optional[str] = None,
    ) -> TenantEnforcementResult:
        """Resolve the final enforcement outcome mapped from the internal reason_code."""
        if not self.decide_enforcement():
            outcome = "APPLIED" if self.mode == "rls_session" else "SKIPPED_NOT_REQUIRED"
            return TenantEnforcementResult(
                applied=self.mode == "rls_session",
                mode=self.mode,
                outcome=outcome,
                reason_code=reason_code,
            )

        normalized = (reason_code or "").strip().upper()

        if normalized == "NO_PREDICATES_PRODUCED":
            return TenantEnforcementResult(
                applied=False,
                mode=self.mode,
                outcome="SKIPPED_NOT_REQUIRED",
                reason_code=None,
            )

        if applied:
            return TenantEnforcementResult(
                applied=True,
                mode=self.mode,
                outcome="APPLIED",
                reason_code=None,
            )

        if normalized == "REWRITE_DISABLED":
            outcome = "REJECTED_DISABLED"
        elif normalized == "REWRITE_TIMEOUT":
            outcome = "REJECTED_TIMEOUT"
        elif normalized in {
            "AST_COMPLEXITY_EXCEEDED",
            "PARAM_LIMIT_EXCEEDED",
            "TARGET_LIMIT_EXCEEDED",
        }:
            outcome = "REJECTED_LIMIT"
        else:
            outcome = "REJECTED_UNSUPPORTED"

        return TenantEnforcementResult(
            applied=False,
            mode=self.mode,
            outcome=outcome,
            reason_code=normalized,
        )
