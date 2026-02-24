"""Provider/mode conformance harness for execution resource containment."""

import pytest

from dal.capabilities import capabilities_for_provider
from dal.resource_containment import ResourceContainmentPolicyError, validate_resource_capabilities
from dal.util.env import PROVIDER_ALIASES

_KNOWN_PROVIDERS = sorted({value for value in PROVIDER_ALIASES.values() if value != "memgraph"})
_PROVIDERS = _KNOWN_PROVIDERS + ["unknown-provider"]
_MODES = [
    ("disabled", False, False, False),
    ("row_only", True, False, False),
    ("byte_only", False, True, False),
    ("timeout_only", False, False, True),
    ("all_enabled", True, True, True),
]


@pytest.mark.parametrize("provider", _PROVIDERS)
@pytest.mark.parametrize("mode, row_enabled, byte_enabled, timeout_enabled", _MODES)
def test_execution_resource_containment_provider_mode_conformance(
    provider: str,
    mode: str,
    row_enabled: bool,
    byte_enabled: bool,
    timeout_enabled: bool,
):
    """Each provider/mode combination should resolve deterministically."""
    caps = capabilities_for_provider(provider)
    should_fail_closed = provider == "unknown-provider" and (
        row_enabled or byte_enabled or timeout_enabled
    )

    if should_fail_closed:
        with pytest.raises(ResourceContainmentPolicyError):
            validate_resource_capabilities(
                provider=provider,
                enforce_row_limit=row_enabled,
                enforce_byte_limit=byte_enabled,
                enforce_timeout=timeout_enabled,
                supports_row_cap=bool(getattr(caps, "supports_row_cap", False)),
                supports_byte_cap=bool(getattr(caps, "supports_byte_cap", False)),
                supports_timeout=bool(getattr(caps, "supports_timeout", False)),
            )
        return

    validate_resource_capabilities(
        provider=provider,
        enforce_row_limit=row_enabled,
        enforce_byte_limit=byte_enabled,
        enforce_timeout=timeout_enabled,
        supports_row_cap=bool(getattr(caps, "supports_row_cap", False)),
        supports_byte_cap=bool(getattr(caps, "supports_byte_cap", False)),
        supports_timeout=bool(getattr(caps, "supports_timeout", False)),
    )
