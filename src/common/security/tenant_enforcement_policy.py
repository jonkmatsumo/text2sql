from dataclasses import dataclass
from typing import Literal


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

    def determine_outcome(self) -> None:
        """Resolve the final enforcement outcome."""
        pass
