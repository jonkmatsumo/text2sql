"""Typed settings for Postgres session-level guardrails."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional

from common.config.env import get_env_bool, get_env_str

SESSION_GUARDRAIL_APPLIED = "SESSION_GUARDRAIL_APPLIED"
SESSION_GUARDRAIL_SKIPPED = "SESSION_GUARDRAIL_SKIPPED"
SESSION_GUARDRAIL_UNSUPPORTED_PROVIDER = "SESSION_GUARDRAIL_UNSUPPORTED_PROVIDER"
SESSION_GUARDRAIL_MISCONFIGURED = "SESSION_GUARDRAIL_MISCONFIGURED"

RESTRICTED_SESSION_MODE_OFF = "off"
RESTRICTED_SESSION_MODE_SET_LOCAL_CONFIG = "set_local_config"


def sanitize_execution_role_name(raw_name: Optional[str]) -> Optional[str]:
    """Return a bounded, normalized execution role identifier safe for telemetry."""
    if raw_name is None:
        return None
    normalized = raw_name.strip().lower()
    if not normalized:
        return None
    sanitized = re.sub(r"[^a-z0-9_]", "_", normalized)
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    if not sanitized:
        sanitized = "invalid_role"
    return sanitized[:63]


def build_session_guardrail_metadata(
    *,
    applied: bool,
    outcome: str,
    execution_role_applied: bool,
    execution_role_name: Optional[str],
    restricted_session_mode: str,
    capability_mismatch: Optional[str] = None,
) -> dict[str, Any]:
    """Build bounded guardrail metadata shared by spans and envelopes."""
    return {
        "session_guardrail_applied": bool(applied),
        "session_guardrail_outcome": outcome,
        "execution_role_applied": bool(execution_role_applied),
        "execution_role_name": sanitize_execution_role_name(execution_role_name),
        "restricted_session_mode": restricted_session_mode,
        "session_guardrail_capability_mismatch": capability_mismatch,
    }


@dataclass(frozen=True)
class PostgresSessionGuardrailSettings:
    """Startup-resolved toggles for Postgres session guardrails."""

    restricted_session_enabled: bool
    execution_role_enabled: bool
    execution_role_name: Optional[str]
    sandbox_enabled: bool = True

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
            sandbox_enabled=bool(get_env_bool("POSTGRES_SANDBOX_ENABLED", True)),
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

        if self.restricted_session_enabled and not self.sandbox_enabled:
            raise ValueError(
                "POSTGRES_RESTRICTED_SESSION_ENABLED=true requires POSTGRES_SANDBOX_ENABLED=true."
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
                outcome=SESSION_GUARDRAIL_UNSUPPORTED_PROVIDER,
                message=(
                    "Restricted session guardrails are not supported for provider "
                    f"'{(provider or '').strip().lower() or 'unknown'}'."
                ),
                envelope_metadata=build_session_guardrail_metadata(
                    applied=False,
                    outcome=SESSION_GUARDRAIL_UNSUPPORTED_PROVIDER,
                    execution_role_applied=False,
                    execution_role_name=self.execution_role_name,
                    restricted_session_mode=RESTRICTED_SESSION_MODE_OFF,
                    capability_mismatch="session_guardrail_restricted_session_unsupported_provider",
                ),
            )
        if self.execution_role_enabled and not supports_execution_role:
            raise SessionGuardrailPolicyError(
                reason_code="session_guardrail_execution_role_unsupported_provider",
                outcome=SESSION_GUARDRAIL_UNSUPPORTED_PROVIDER,
                message=(
                    "Execution-role guardrails are not supported for provider "
                    f"'{(provider or '').strip().lower() or 'unknown'}'."
                ),
                envelope_metadata=build_session_guardrail_metadata(
                    applied=False,
                    outcome=SESSION_GUARDRAIL_UNSUPPORTED_PROVIDER,
                    execution_role_applied=False,
                    execution_role_name=self.execution_role_name,
                    restricted_session_mode=RESTRICTED_SESSION_MODE_OFF,
                    capability_mismatch="session_guardrail_execution_role_unsupported_provider",
                ),
            )


class SessionGuardrailPolicyError(RuntimeError):
    """Deterministic policy exception for session guardrail capability mismatches."""

    def __init__(
        self,
        *,
        reason_code: str,
        outcome: str,
        message: str,
        envelope_metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        """Initialize a bounded session-guardrail policy exception."""
        super().__init__(message)
        self.reason_code = reason_code
        self.outcome = outcome
        self.envelope_metadata = envelope_metadata or build_session_guardrail_metadata(
            applied=False,
            outcome=outcome,
            execution_role_applied=False,
            execution_role_name=None,
            restricted_session_mode=RESTRICTED_SESSION_MODE_OFF,
            capability_mismatch=reason_code,
        )
