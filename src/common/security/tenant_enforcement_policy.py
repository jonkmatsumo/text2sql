from dataclasses import dataclass
from typing import Literal, Optional


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

    def classify_sql(self, sql: str) -> None:
        """Classify the provided SQL shape for enforcement."""
        pass

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
