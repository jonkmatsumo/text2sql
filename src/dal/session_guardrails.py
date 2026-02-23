"""Typed settings for Postgres session-level guardrails."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from common.config.env import get_env_bool, get_env_str


@dataclass(frozen=True)
class PostgresSessionGuardrailSettings:
    """Startup-resolved toggles for Postgres session guardrails."""

    restricted_session_enabled: bool
    execution_role_enabled: bool
    execution_role_name: Optional[str]

    @classmethod
    def from_env(cls) -> "PostgresSessionGuardrailSettings":
        """Build settings from environment variables."""
        execution_role_name = (get_env_str("POSTGRES_EXECUTION_ROLE", "") or "").strip() or None
        return cls(
            restricted_session_enabled=bool(
                get_env_bool("POSTGRES_RESTRICTED_SESSION_ENABLED", False)
            ),
            execution_role_enabled=bool(get_env_bool("POSTGRES_EXECUTION_ROLE_ENABLED", False)),
            execution_role_name=execution_role_name,
        )

    def validate_basic(self, provider: str) -> None:
        """Fail closed on invalid combinations independent of provider capability map."""
        normalized_provider = (provider or "").strip().lower()
        if self.execution_role_enabled and not self.execution_role_name:
            raise ValueError(
                "POSTGRES_EXECUTION_ROLE_ENABLED=true requires POSTGRES_EXECUTION_ROLE to be set."
            )

        if self.restricted_session_enabled and normalized_provider != "postgres":
            raise ValueError(
                "POSTGRES_RESTRICTED_SESSION_ENABLED=true is only supported for provider=postgres."
            )

        if self.execution_role_enabled and normalized_provider != "postgres":
            raise ValueError(
                "POSTGRES_EXECUTION_ROLE_ENABLED=true is only supported for provider=postgres."
            )

    def validate_capabilities(
        self,
        *,
        provider: str,
        supports_restricted_session: bool,
        supports_execution_role: bool,
    ) -> None:
        """Fail closed when enabled guardrails are unsupported by provider capabilities."""
        if self.restricted_session_enabled and not supports_restricted_session:
            raise SessionGuardrailPolicyError(
                reason_code="session_guardrail_restricted_session_unsupported_provider",
                outcome="SESSION_GUARDRAIL_UNSUPPORTED_PROVIDER",
                message=(
                    "Restricted session guardrails are not supported for provider "
                    f"'{(provider or '').strip().lower() or 'unknown'}'."
                ),
            )
        if self.execution_role_enabled and not supports_execution_role:
            raise SessionGuardrailPolicyError(
                reason_code="session_guardrail_execution_role_unsupported_provider",
                outcome="SESSION_GUARDRAIL_UNSUPPORTED_PROVIDER",
                message=(
                    "Execution-role guardrails are not supported for provider "
                    f"'{(provider or '').strip().lower() or 'unknown'}'."
                ),
            )


class SessionGuardrailPolicyError(RuntimeError):
    """Deterministic policy exception for session guardrail capability mismatches."""

    def __init__(self, *, reason_code: str, outcome: str, message: str) -> None:
        """Initialize a bounded session-guardrail policy exception."""
        super().__init__(message)
        self.reason_code = reason_code
        self.outcome = outcome
