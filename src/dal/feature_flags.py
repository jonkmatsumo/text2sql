from common.config.env import get_env_bool


def experimental_features_enabled() -> bool:
    """Return True when experimental DAL features are enabled."""
    return get_env_bool("DAL_EXPERIMENTAL_FEATURES", False)


def allow_non_postgres_tenant_bypass() -> bool:
    """Return True when legacy non-Postgres tenant bypass is explicitly enabled."""
    return bool(get_env_bool("ALLOW_NON_POSTGRES_TENANT_BYPASS", False))
