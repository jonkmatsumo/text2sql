"""Provider/mode conformance harness for Postgres session guardrails."""

import pytest

from dal.capabilities import capabilities_for_provider
from dal.session_guardrails import PostgresSessionGuardrailSettings, SessionGuardrailPolicyError
from dal.util.env import PROVIDER_ALIASES

_PROVIDERS = sorted({value for value in PROVIDER_ALIASES.values() if value != "memgraph"})
_ROLE = "text2sql_readonly"

_MODES = [
    ("disabled", False, False, None),
    ("restricted_only", True, False, None),
    ("role_only", False, True, _ROLE),
    ("restricted_and_role", True, True, _ROLE),
    ("role_missing_name", False, True, None),
]


def _startup_outcome(
    provider: str,
    *,
    restricted_enabled: bool,
    role_enabled: bool,
    role_name: str | None,
) -> str:
    settings = PostgresSessionGuardrailSettings(
        restricted_session_enabled=restricted_enabled,
        execution_role_enabled=role_enabled,
        execution_role_name=role_name,
    )

    try:
        settings.validate_basic(provider)
    except ValueError:
        return "SESSION_GUARDRAIL_MISCONFIGURED"

    caps = capabilities_for_provider(provider)
    try:
        settings.validate_capabilities(
            provider=provider,
            supports_restricted_session=bool(getattr(caps, "supports_restricted_session", False)),
            supports_execution_role=bool(getattr(caps, "supports_execution_role", False)),
        )
    except SessionGuardrailPolicyError as exc:
        return exc.outcome

    if restricted_enabled or role_enabled:
        return "SESSION_GUARDRAIL_APPLIED"
    return "SESSION_GUARDRAIL_SKIPPED"


def _expected_startup_outcome(
    provider: str,
    *,
    restricted_enabled: bool,
    role_enabled: bool,
    role_name: str | None,
) -> str:
    if role_enabled and not role_name:
        return "SESSION_GUARDRAIL_MISCONFIGURED"
    if not restricted_enabled and not role_enabled:
        return "SESSION_GUARDRAIL_SKIPPED"
    if provider == "postgres":
        return "SESSION_GUARDRAIL_APPLIED"
    return "SESSION_GUARDRAIL_MISCONFIGURED"


@pytest.mark.parametrize("provider", _PROVIDERS)
@pytest.mark.parametrize(
    "mode, restricted_enabled, role_enabled, role_name",
    _MODES,
)
def test_postgres_session_guardrail_startup_conformance_matrix(
    provider: str,
    mode: str,
    restricted_enabled: bool,
    role_enabled: bool,
    role_name: str | None,
):
    """Every provider/mode combination should resolve to a deterministic outcome."""
    observed = _startup_outcome(
        provider,
        restricted_enabled=restricted_enabled,
        role_enabled=role_enabled,
        role_name=role_name,
    )
    expected = _expected_startup_outcome(
        provider,
        restricted_enabled=restricted_enabled,
        role_enabled=role_enabled,
        role_name=role_name,
    )
    assert (
        observed == expected
    ), f"provider={provider} mode={mode} expected={expected} observed={observed}"


def test_postgres_session_guardrail_unknown_provider_capability_fails_closed():
    """Unknown provider capability payloads should fail closed during capability checks."""
    provider = "unknown-provider"
    caps = capabilities_for_provider(provider)
    settings = PostgresSessionGuardrailSettings(
        restricted_session_enabled=True,
        execution_role_enabled=False,
        execution_role_name=None,
    )

    with pytest.raises(SessionGuardrailPolicyError) as exc_info:
        settings.validate_capabilities(
            provider=provider,
            supports_restricted_session=bool(getattr(caps, "supports_restricted_session", False)),
            supports_execution_role=bool(getattr(caps, "supports_execution_role", False)),
        )
    assert exc_info.value.outcome == "SESSION_GUARDRAIL_UNSUPPORTED_PROVIDER"
