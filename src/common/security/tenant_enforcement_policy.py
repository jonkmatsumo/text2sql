from __future__ import annotations

import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Awaitable, Callable, Mapping, Optional, Sequence

from common.sql.tenant_sql_rewriter import TenantRewriteSettings

_TENANT_REWRITE_REASON_ALLOWLIST = {
    "AST_COMPLEXITY_EXCEEDED",
    "COMPLETENESS_FAILED",
    "CORRELATED_SUBQUERY_UNSUPPORTED",
    "CTE_UNSUPPORTED_SHAPE",
    "MISSING_TENANT_COLUMN",
    "MISSING_TENANT_COLUMN_CONFIG",
    "NESTED_FROM_UNSUPPORTED",
    "NO_PREDICATES_PRODUCED",
    "NOT_SELECT_STATEMENT",
    "NOT_SINGLE_SELECT",
    "PARAM_LIMIT_EXCEEDED",
    "PARSE_ERROR",
    "PROVIDER_UNSUPPORTED",
    "REWRITE_DISABLED",
    "REWRITE_TIMEOUT",
    "SET_OPERATIONS_UNSUPPORTED",
    "SUBQUERY_UNSUPPORTED",
    "TENANT_ID_REQUIRED",
    "TARGET_LIMIT_EXCEEDED",
    "TENANT_MODE_UNSUPPORTED",
    "UNRESOLVABLE_TABLE_ALIAS",
    "UNRESOLVABLE_TABLE_IDENTITY",
    "WINDOW_FUNCTIONS_UNSUPPORTED",
}
_TENANT_REWRITE_INTERNAL_REASON_ALLOWLIST = {
    "SCHEMA_METADATA_MISSING",
    "SCHEMA_TENANT_COLUMN_MISSING",
}
_TENANT_REWRITE_LIMIT_REASONS = {
    "AST_COMPLEXITY_EXCEEDED",
    "PARAM_LIMIT_EXCEEDED",
    "TARGET_LIMIT_EXCEEDED",
}
_TENANT_ENFORCEMENT_MODE_ALLOWLIST = {"sql_rewrite", "rls_session", "none"}
_TENANT_ENFORCEMENT_OUTCOME_ALLOWLIST = {
    "APPLIED",
    "SKIPPED_NOT_REQUIRED",
    "REJECTED_UNSUPPORTED",
    "REJECTED_DISABLED",
    "REJECTED_LIMIT",
    "REJECTED_MISSING_TENANT",
    "REJECTED_TIMEOUT",
}


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
class TenantSQLShapeClassifier:
    """Classifier for SQL shape enforcement eligibility."""

    strict: bool
    max_ast_nodes: int

    def classify(self, sql: str, provider: str = "sqlite") -> TenantSQLShape:
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
        ast_node_count = sum(1 for _ in expression.walk())
        if ast_node_count > self.max_ast_nodes:
            return TenantSQLShape.UNSUPPORTED_COMPLEXITY

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
        if has_cte_args and classify_cte_query(expression) == CTEClassification.UNSUPPORTED_CTE:
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


@dataclass(frozen=True)
class TenantEnforcementResult:
    """Result payload mapping enforcement outcomes."""

    applied: bool
    mode: str
    outcome: str
    reason_code: Optional[str]


@dataclass(frozen=True)
class PolicyDecision:
    """Final policy decision consumed by execute_sql_query orchestration."""

    result: TenantEnforcementResult
    sql_to_execute: str
    params_to_bind: list[Any]
    should_execute: bool
    envelope_metadata: dict[str, Any]
    telemetry_attributes: dict[str, Any]
    metric_attributes: dict[str, Any]
    bounded_reason_code: Optional[str]
    tenant_required: bool
    would_apply_rewrite: bool


@dataclass(frozen=True)
class TenantEnforcementPolicy:
    """Policy for determining tenant enforcement rules and outcomes."""

    provider: str
    mode: str
    strict: bool
    max_targets: int
    max_params: int
    max_ast_nodes: int
    hard_timeout_ms: int
    warn_ms: int = 50
    rewrite_enabled: bool = True

    def decide_enforcement(self) -> bool:
        """Evaluate if enforcement should occur through SQL rewrite."""
        return self._normalized_mode_raw() == "sql_rewrite"

    def classify_sql(self, sql: str, provider: str = "sqlite") -> TenantSQLShape:
        """Classify the provided SQL shape for enforcement capability."""
        return TenantSQLShapeClassifier(
            strict=self.strict, max_ast_nodes=self.max_ast_nodes
        ).classify(
            sql,
            provider=provider,
        )

    def determine_outcome(
        self,
        *,
        applied: bool,
        reason_code: Optional[str] = None,
    ) -> TenantEnforcementResult:
        """Resolve the final enforcement outcome mapped from the internal reason_code."""
        normalized_mode = self._normalized_mode_raw()
        normalized_reason = self._normalized_reason_code(reason_code)

        if normalized_reason == "TENANT_ID_REQUIRED":
            return TenantEnforcementResult(
                applied=False,
                mode="none" if normalized_mode == "unsupported" else normalized_mode,
                outcome="REJECTED_MISSING_TENANT",
                reason_code="TENANT_ID_REQUIRED",
            )

        if normalized_mode == "unsupported":
            return TenantEnforcementResult(
                applied=False,
                mode="none",
                outcome="REJECTED_UNSUPPORTED",
                reason_code=normalized_reason or "TENANT_MODE_UNSUPPORTED",
            )

        if not self.decide_enforcement():
            outcome = "APPLIED" if normalized_mode == "rls_session" else "SKIPPED_NOT_REQUIRED"
            return TenantEnforcementResult(
                applied=normalized_mode == "rls_session",
                mode=normalized_mode,
                outcome=outcome,
                reason_code=None,
            )

        if normalized_reason == "NO_PREDICATES_PRODUCED":
            return TenantEnforcementResult(
                applied=False,
                mode="sql_rewrite",
                outcome="SKIPPED_NOT_REQUIRED",
                reason_code=None,
            )

        if applied:
            return TenantEnforcementResult(
                applied=True,
                mode="sql_rewrite",
                outcome="APPLIED",
                reason_code=None,
            )

        if normalized_reason == "REWRITE_DISABLED":
            outcome = "REJECTED_DISABLED"
        elif normalized_reason == "REWRITE_TIMEOUT":
            outcome = "REJECTED_TIMEOUT"
        elif normalized_reason in _TENANT_REWRITE_LIMIT_REASONS:
            outcome = "REJECTED_LIMIT"
        else:
            outcome = "REJECTED_UNSUPPORTED"

        return TenantEnforcementResult(
            applied=False,
            mode="sql_rewrite",
            outcome=outcome,
            reason_code=normalized_reason,
        )

    def default_decision(self, *, sql: str, params: Sequence[Any] | None = None) -> PolicyDecision:
        """Build default policy metadata prior to SQL rewrite evaluation."""
        normalized_mode = self._normalized_mode_raw()
        if normalized_mode == "unsupported":
            result = self.determine_outcome(applied=False, reason_code="TENANT_MODE_UNSUPPORTED")
            return self._build_decision(
                result=result,
                sql_to_execute=sql,
                params_to_bind=list(params or []),
                should_execute=False,
                tenant_required=False,
                would_apply_rewrite=False,
            )

        result = self.determine_outcome(applied=normalized_mode == "rls_session", reason_code=None)
        return self._build_decision(
            result=result,
            sql_to_execute=sql,
            params_to_bind=list(params or []),
            should_execute=True,
            tenant_required=normalized_mode == "rls_session",
            would_apply_rewrite=False,
        )

    async def evaluate(
        self,
        *,
        sql: str,
        tenant_id: int | None,
        params: Sequence[Any] | None = None,
        tenant_column: str = "tenant_id",
        global_table_allowlist: set[str] | None = None,
        simulate: bool = False,
        schema_snapshot_loader: (
            Callable[[Sequence[str], int], Awaitable[Mapping[str, set[str]]]] | None
        ) = None,
    ) -> PolicyDecision:
        """Evaluate tenant enforcement and return a single execution decision payload."""
        normalized_mode = self._normalized_mode_raw()
        current_params = list(params or [])

        if normalized_mode == "unsupported":
            return self._reject_decision(
                sql=sql,
                params=current_params,
                reason_code="TENANT_MODE_UNSUPPORTED",
                tenant_required=False,
                would_apply_rewrite=False,
            )

        if normalized_mode == "none":
            result = self.determine_outcome(
                applied=normalized_mode == "rls_session", reason_code=None
            )
            return self._build_decision(
                result=result,
                sql_to_execute=sql,
                params_to_bind=current_params,
                should_execute=True,
                tenant_required=False,
                would_apply_rewrite=False,
            )

        if normalized_mode == "rls_session":
            if tenant_id is None:
                return self._reject_decision(
                    sql=sql,
                    params=current_params,
                    reason_code="TENANT_ID_REQUIRED",
                    tenant_required=True,
                    would_apply_rewrite=False,
                )
            result = self.determine_outcome(applied=True, reason_code=None)
            return self._build_decision(
                result=result,
                sql_to_execute=sql,
                params_to_bind=current_params,
                should_execute=True,
                tenant_required=True,
                would_apply_rewrite=False,
            )

        if not self.rewrite_enabled:
            return self._reject_decision(
                sql=sql,
                params=current_params,
                reason_code="REWRITE_DISABLED",
                tenant_required=False,
                would_apply_rewrite=False,
            )

        if simulate:
            simulated_telemetry = {"tenant.policy.simulated": True}
            classification_reason_code = self._classification_reason_code(
                self.classify_sql(sql, provider=self.provider)
            )
            if classification_reason_code is not None:
                return self._reject_decision(
                    sql=sql,
                    params=current_params,
                    reason_code=classification_reason_code,
                    tenant_required=False,
                    would_apply_rewrite=False,
                    extra_telemetry=simulated_telemetry,
                )

            would_apply_rewrite = self._would_apply_rewrite(
                sql,
                provider=self.provider,
                global_table_allowlist=global_table_allowlist,
            )
            if not would_apply_rewrite:
                result = self.determine_outcome(applied=False, reason_code="NO_PREDICATES_PRODUCED")
                return self._build_decision(
                    result=result,
                    sql_to_execute=sql,
                    params_to_bind=current_params,
                    should_execute=True,
                    tenant_required=False,
                    would_apply_rewrite=False,
                    extra_telemetry=simulated_telemetry,
                )
            if tenant_id is None:
                return self._reject_decision(
                    sql=sql,
                    params=current_params,
                    reason_code="TENANT_ID_REQUIRED",
                    tenant_required=True,
                    would_apply_rewrite=True,
                    extra_telemetry=simulated_telemetry,
                )

            result = self.determine_outcome(applied=True, reason_code=None)
            return self._build_decision(
                result=result,
                sql_to_execute=sql,
                params_to_bind=current_params,
                should_execute=True,
                tenant_required=True,
                would_apply_rewrite=True,
                extra_telemetry=simulated_telemetry,
            )

        shape = self.classify_sql(sql, provider=self.provider)
        classification_reason_code = self._classification_reason_code(shape)
        if classification_reason_code is not None:
            return self._reject_decision(
                sql=sql,
                params=current_params,
                reason_code=classification_reason_code,
                tenant_required=False,
                would_apply_rewrite=False,
            )

        from common.sql.tenant_sql_rewriter import (
            CTEClassification,
            TenantSQLTransformerError,
            transform_tenant_scoped_sql,
        )

        rewrite_tenant_id = tenant_id if tenant_id is not None else 0
        cte_classification = (
            CTEClassification.SAFE_SIMPLE_CTE if shape == TenantSQLShape.SAFE_CTE_QUERY else None
        )
        rewrite_started = time.perf_counter()
        try:
            rewrite_result = transform_tenant_scoped_sql(
                sql,
                provider=self.provider,
                tenant_id=rewrite_tenant_id,
                tenant_column=tenant_column,
                global_table_allowlist=global_table_allowlist,
                max_targets=self.max_targets,
                max_params=self.max_params,
                max_ast_nodes=self.max_ast_nodes,
                cte_classification=cte_classification,
                assert_invariants=False,
            )
        except TenantSQLTransformerError as exc:
            rewrite_duration_ms = (time.perf_counter() - rewrite_started) * 1000
            telemetry_attrs = self._rewrite_duration_attributes(rewrite_duration_ms)
            normalized_reason = self._reason_code_for_transformer_kind(exc.kind)
            if normalized_reason == "NO_PREDICATES_PRODUCED":
                result = self.determine_outcome(applied=False, reason_code=normalized_reason)
                return self._build_decision(
                    result=result,
                    sql_to_execute=sql,
                    params_to_bind=current_params,
                    should_execute=True,
                    tenant_required=False,
                    would_apply_rewrite=False,
                    extra_telemetry=telemetry_attrs,
                )
            if tenant_id is None:
                return self._reject_decision(
                    sql=sql,
                    params=current_params,
                    reason_code="TENANT_ID_REQUIRED",
                    tenant_required=True,
                    would_apply_rewrite=True,
                    extra_telemetry=telemetry_attrs,
                )
            return self._reject_decision(
                sql=sql,
                params=current_params,
                reason_code=normalized_reason,
                tenant_required=False,
                would_apply_rewrite=False,
                extra_telemetry=telemetry_attrs,
            )

        rewrite_duration_ms = (time.perf_counter() - rewrite_started) * 1000
        telemetry_attrs = self._rewrite_duration_attributes(rewrite_duration_ms)
        if rewrite_duration_ms > self.hard_timeout_ms:
            if tenant_id is None:
                return self._reject_decision(
                    sql=sql,
                    params=current_params,
                    reason_code="TENANT_ID_REQUIRED",
                    tenant_required=True,
                    would_apply_rewrite=True,
                    extra_telemetry=telemetry_attrs,
                )
            return self._reject_decision(
                sql=sql,
                params=current_params,
                reason_code="REWRITE_TIMEOUT",
                tenant_required=False,
                would_apply_rewrite=False,
                extra_telemetry=telemetry_attrs,
            )

        tenant_required = bool(rewrite_result.tenant_predicates_added > 0)
        if tenant_id is None and tenant_required:
            return self._reject_decision(
                sql=sql,
                params=current_params,
                reason_code="TENANT_ID_REQUIRED",
                tenant_required=True,
                would_apply_rewrite=True,
                extra_telemetry=telemetry_attrs,
            )

        if (
            schema_snapshot_loader is not None
            and rewrite_result.tables_rewritten
            and tenant_id is not None
        ):
            table_columns = await schema_snapshot_loader(rewrite_result.tables_rewritten, tenant_id)
            missing_schema_tables, missing_tenant_column_tables = self._schema_validation_failures(
                rewrite_result.tables_rewritten,
                table_columns,
                tenant_column,
            )
            if missing_schema_tables or missing_tenant_column_tables:
                schema_reason_code = (
                    "SCHEMA_TENANT_COLUMN_MISSING"
                    if missing_tenant_column_tables
                    else "SCHEMA_METADATA_MISSING"
                )
                return self._reject_decision(
                    sql=sql,
                    params=current_params,
                    reason_code=schema_reason_code,
                    tenant_required=tenant_required,
                    would_apply_rewrite=tenant_required,
                    extra_telemetry=telemetry_attrs,
                )

        telemetry_attrs.update(
            {
                "rewrite.target_count": int(rewrite_result.target_count),
                "rewrite.param_count": int(len(rewrite_result.params)),
                "rewrite.scope_depth": int(rewrite_result.scope_depth),
                "rewrite.has_cte": bool(rewrite_result.has_cte),
                "rewrite.has_subquery": bool(rewrite_result.has_subquery),
            }
        )

        result = self.determine_outcome(
            applied=tenant_required,
            reason_code=None,
        )
        current_params.extend(rewrite_result.params)
        return self._build_decision(
            result=result,
            sql_to_execute=rewrite_result.rewritten_sql,
            params_to_bind=current_params,
            should_execute=True,
            tenant_required=tenant_required,
            would_apply_rewrite=tenant_required,
            extra_telemetry=telemetry_attrs,
        )

    def bounded_reason_code(self, reason_code: str | None) -> str | None:
        """Return a bounded reason code for metadata and telemetry."""
        normalized = self._normalized_reason_code(reason_code)
        if normalized is None:
            return None
        if normalized in _TENANT_REWRITE_REASON_ALLOWLIST:
            return f"tenant_rewrite_{normalized.lower()}"
        if normalized in _TENANT_REWRITE_INTERNAL_REASON_ALLOWLIST:
            return f"tenant_rewrite_{normalized.lower()}"
        return "tenant_enforcement_unsupported"

    def _reject_decision(
        self,
        *,
        sql: str,
        params: Sequence[Any],
        reason_code: str,
        tenant_required: bool,
        would_apply_rewrite: bool,
        extra_telemetry: dict[str, Any] | None = None,
    ) -> PolicyDecision:
        result = self.determine_outcome(applied=False, reason_code=reason_code)
        telemetry_attrs = dict(extra_telemetry or {})
        normalized_reason = self._normalized_reason_code(reason_code) or "UNKNOWN"
        telemetry_attrs["tenant_rewrite.failure_reason"] = normalized_reason
        bounded_reason = self.bounded_reason_code(normalized_reason)
        if bounded_reason is not None:
            telemetry_attrs["tenant_rewrite.failure_reason_code"] = bounded_reason
        return self._build_decision(
            result=result,
            sql_to_execute=sql,
            params_to_bind=list(params),
            should_execute=False,
            bounded_reason_override=bounded_reason,
            tenant_required=tenant_required,
            would_apply_rewrite=would_apply_rewrite,
            extra_telemetry=telemetry_attrs,
        )

    def _build_decision(
        self,
        *,
        result: TenantEnforcementResult,
        sql_to_execute: str,
        params_to_bind: list[Any],
        should_execute: bool,
        tenant_required: bool,
        would_apply_rewrite: bool,
        bounded_reason_override: str | None = None,
        extra_telemetry: dict[str, Any] | None = None,
    ) -> PolicyDecision:
        bounded_reason = (
            bounded_reason_override
            if bounded_reason_override is not None
            else self.bounded_reason_code(result.reason_code)
        )
        mode = self._normalize_mode_for_envelope(result.mode)
        outcome = self._normalize_outcome(result.outcome)
        envelope_metadata: dict[str, Any] = {
            "tenant_enforcement_applied": bool(result.applied),
            "tenant_enforcement_mode": mode,
            "tenant_rewrite_outcome": outcome,
        }
        if bounded_reason:
            envelope_metadata["tenant_rewrite_reason_code"] = bounded_reason

        telemetry_attrs: dict[str, Any] = {
            "tenant.enforcement.mode": mode,
            "tenant.enforcement.outcome": outcome,
            "tenant.enforcement.applied": bool(result.applied),
        }
        if bounded_reason:
            telemetry_attrs["tenant.enforcement.reason_code"] = bounded_reason
        if extra_telemetry:
            telemetry_attrs.update(extra_telemetry)

        metric_attributes: dict[str, Any] = {
            "mode": mode,
            "outcome": outcome,
            "applied": bool(result.applied),
        }
        if bounded_reason:
            metric_attributes["reason_code"] = bounded_reason

        return PolicyDecision(
            result=result,
            sql_to_execute=sql_to_execute,
            params_to_bind=list(params_to_bind),
            should_execute=should_execute,
            envelope_metadata=envelope_metadata,
            telemetry_attributes=telemetry_attrs,
            metric_attributes=metric_attributes,
            bounded_reason_code=bounded_reason,
            tenant_required=tenant_required,
            would_apply_rewrite=would_apply_rewrite,
        )

    def _rewrite_duration_attributes(self, rewrite_duration_ms: float) -> dict[str, Any]:
        return {
            "rewrite.duration_ms": round(rewrite_duration_ms, 3),
            "rewrite.duration_warn_exceeded": rewrite_duration_ms > self.warn_ms,
        }

    def _schema_validation_failures(
        self,
        table_names: Sequence[str],
        table_columns: Mapping[str, set[str]],
        tenant_column: str,
    ) -> tuple[list[str], list[str]]:
        tenant_column_normalized = tenant_column.strip().lower()
        if not tenant_column_normalized:
            return [], []

        missing_schema: list[str] = []
        missing_tenant_column: list[str] = []
        for table_name in table_names:
            normalized = (table_name or "").strip().lower()
            if not normalized:
                continue
            columns = table_columns.get(normalized)
            if columns is None:
                columns = table_columns.get(normalized.split(".")[-1])
            if columns is None:
                missing_schema.append(normalized)
                continue
            if tenant_column_normalized not in columns:
                missing_tenant_column.append(normalized)
        return missing_schema, missing_tenant_column

    def _normalized_mode_raw(self) -> str:
        normalized = (self.mode or "").strip().lower()
        if normalized in _TENANT_ENFORCEMENT_MODE_ALLOWLIST:
            return normalized
        if normalized == "unsupported":
            return "unsupported"
        return "none"

    def _normalize_mode_for_envelope(self, mode: str | None) -> str:
        normalized = (mode or "").strip().lower()
        if normalized in _TENANT_ENFORCEMENT_MODE_ALLOWLIST:
            return normalized
        return "none"

    def _normalize_outcome(self, outcome: str | None) -> str:
        normalized = (outcome or "").strip().upper()
        if normalized in _TENANT_ENFORCEMENT_OUTCOME_ALLOWLIST:
            return normalized
        return "REJECTED_UNSUPPORTED"

    def _normalized_reason_code(self, reason_code: str | None) -> str | None:
        normalized = (reason_code or "").strip().upper()
        return normalized if normalized else None

    def _reason_code_for_transformer_kind(self, kind: object) -> str:
        value = getattr(kind, "value", kind)
        normalized = self._normalized_reason_code(str(value))
        return normalized or "UNKNOWN_ERROR"

    def _classification_reason_code(self, shape: TenantSQLShape) -> str | None:
        if shape == TenantSQLShape.UNSUPPORTED_SET_OPERATION:
            return "SET_OPERATIONS_UNSUPPORTED"
        if shape == TenantSQLShape.UNSUPPORTED_STATEMENT_TYPE:
            return "NOT_SELECT_STATEMENT"
        if shape == TenantSQLShape.UNSUPPORTED_CTE:
            return "CTE_UNSUPPORTED_SHAPE"
        if shape == TenantSQLShape.UNSUPPORTED_WINDOW_FUNCTION:
            return "WINDOW_FUNCTIONS_UNSUPPORTED"
        if shape == TenantSQLShape.UNSUPPORTED_NESTED_FROM:
            return "NESTED_FROM_UNSUPPORTED"
        if shape == TenantSQLShape.UNSUPPORTED_CORRELATED_SUBQUERY:
            return "CORRELATED_SUBQUERY_UNSUPPORTED"
        if shape == TenantSQLShape.UNSUPPORTED_SUBQUERY:
            return "SUBQUERY_UNSUPPORTED"
        if shape == TenantSQLShape.UNSUPPORTED_COMPLEXITY:
            return "AST_COMPLEXITY_EXCEEDED"
        if shape == TenantSQLShape.PARSE_ERROR:
            return "PARSE_ERROR"
        return None

    def _would_apply_rewrite(
        self,
        sql: str,
        *,
        provider: str,
        global_table_allowlist: set[str] | None,
    ) -> bool:
        import sqlglot
        from sqlglot import exp

        from common.sql.dialect import normalize_sqlglot_dialect

        dialect = normalize_sqlglot_dialect((provider or "").strip().lower())
        try:
            expression = sqlglot.parse_one(sql, read=dialect)
        except Exception:
            return False
        if not isinstance(expression, exp.Select):
            return False

        with_clause = expression.args.get("with_")
        cte_names = (
            {cte.alias_or_name.lower() for cte in with_clause.expressions if cte.alias_or_name}
            if with_clause
            else set()
        )
        allowlist = {entry.strip().lower() for entry in (global_table_allowlist or set()) if entry}

        for table in expression.find_all(exp.Table):
            alias_or_name = (table.alias_or_name or table.name or "").strip().lower()
            physical_name = (table.name or "").strip().lower()
            if not alias_or_name and not physical_name:
                continue
            candidates = {alias_or_name, physical_name}
            candidates.discard("")
            if candidates.intersection(cte_names):
                continue
            if allowlist and candidates.intersection(allowlist):
                continue
            return True
        return False


__all__ = [
    "PolicyDecision",
    "TenantEnforcementPolicy",
    "TenantEnforcementResult",
    "TenantRewriteSettings",
    "TenantSQLShape",
    "TenantSQLShapeClassifier",
]
