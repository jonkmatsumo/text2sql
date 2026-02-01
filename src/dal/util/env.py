"""Provider normalization and environment variable helpers.

This module provides utilities for reading and normalizing provider
environment variables used by the DAL and Retriever factories.

Canonical Provider IDs (internal, lowercase):
- "postgres" - PostgreSQL implementations
- "memgraph" - Memgraph/Neo4j implementations

User-Facing Aliases (case-insensitive):
- PostgreSQL: "postgresql", "postgres", "pg"
- Memgraph: "memgraph"

Example:
    >>> normalize_provider("PostgreSQL")
    'postgres'
    >>> normalize_provider("PG")
    'postgres'
    >>> get_provider_env("CACHE_STORE_PROVIDER", "postgres", {"postgres", "memgraph"})
    'postgres'
"""

from typing import Set

# Alias mappings: user-friendly names -> canonical provider ID
PROVIDER_ALIASES: dict[str, str] = {
    # PostgreSQL aliases
    "postgresql": "postgres",
    "postgres": "postgres",
    "pg": "postgres",
    # SQLite aliases
    "sqlite": "sqlite",
    "sqlite3": "sqlite",
    # MySQL aliases
    "mysql": "mysql",
    "mariadb": "mysql",
    # Memgraph aliases
    "memgraph": "memgraph",
}


def normalize_provider(value: str) -> str:
    """Normalize a provider value to its canonical form.

    Performs the following transformations:
    1. Strips leading/trailing whitespace
    2. Converts to lowercase
    3. Maps known aliases to canonical provider IDs

    Unknown values pass through unchanged (validation happens separately).

    Args:
        value: Raw provider value (e.g., "PostgreSQL", "pg", "MEMGRAPH").

    Returns:
        Canonical provider ID (e.g., "postgres", "memgraph") or the
        lowercased/stripped input if no alias mapping exists.

    Example:
        >>> normalize_provider("PostgreSQL")
        'postgres'
        >>> normalize_provider("  PG  ")
        'postgres'
        >>> normalize_provider("custom-provider")
        'custom-provider'
    """
    cleaned = value.strip().lower()
    return PROVIDER_ALIASES.get(cleaned, cleaned)


def get_provider_env(var_name: str, default: str, allowed: Set[str]) -> str:
    """Read and validate a provider environment variable.

    Reads the specified environment variable, normalizes the value,
    and validates it against the set of allowed canonical provider IDs.

    Args:
        var_name: Name of the environment variable (e.g., "CACHE_STORE_PROVIDER").
        default: Default canonical provider ID if env var is not set.
        allowed: Set of valid canonical provider IDs (e.g., {"postgres", "memgraph"}).

    Returns:
        The normalized, validated canonical provider ID.

    Raises:
        ValueError: If the normalized value is not in the allowed set.
            The error message includes the env var name, provided value,
            and list of allowed values.

    Example:
        >>> os.environ["CACHE_STORE_PROVIDER"] = "PostgreSQL"
        >>> get_provider_env("CACHE_STORE_PROVIDER", "postgres", {"postgres", "memgraph"})
        'postgres'

        >>> os.environ["CACHE_STORE_PROVIDER"] = "invalid"
        >>> get_provider_env("CACHE_STORE_PROVIDER", "postgres", {"postgres", "memgraph"})
        ValueError: Invalid provider for CACHE_STORE_PROVIDER: 'invalid'.
                    Allowed values: postgres, memgraph
    """
    from common.config.env import get_env_str

    raw_value = get_env_str(var_name)

    if raw_value is None:
        return default

    normalized = normalize_provider(raw_value)

    if normalized not in allowed:
        allowed_list = ", ".join(sorted(allowed))
        raise ValueError(
            f"Invalid provider for {var_name}: '{raw_value}'. " f"Allowed values: {allowed_list}"
        )

    return normalized
