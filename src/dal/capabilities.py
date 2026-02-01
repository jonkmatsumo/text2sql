from dataclasses import dataclass


@dataclass(frozen=True)
class BackendCapabilities:
    """Capability flags for query-target backends."""

    supports_arrays: bool = True
    supports_json_ops: bool = True
    supports_transactions: bool = True
    supports_fk_enforcement: bool = True


def capabilities_for_provider(provider: str) -> BackendCapabilities:
    """Return capability flags for a given query-target provider."""
    normalized = provider.lower()
    if normalized == "redshift":
        return BackendCapabilities(
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=False,
            supports_fk_enforcement=False,
        )
    if normalized == "mysql":
        return BackendCapabilities(
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=True,
            supports_fk_enforcement=False,
        )
    if normalized == "sqlite":
        return BackendCapabilities(
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=True,
            supports_fk_enforcement=False,
        )
    if normalized == "snowflake":
        return BackendCapabilities(
            supports_arrays=False,
            supports_json_ops=False,
            supports_transactions=False,
            supports_fk_enforcement=False,
        )
    return BackendCapabilities()
