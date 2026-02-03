from common.config.env import get_env_bool


def experimental_features_enabled() -> bool:
    """Return True when experimental DAL features are enabled."""
    return get_env_bool("DAL_EXPERIMENTAL_FEATURES", False)
