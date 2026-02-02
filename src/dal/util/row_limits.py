from common.config.env import get_env_int


def get_sync_max_rows() -> int:
    """Return the optional sync max rows guardrail (0 disables)."""
    value = get_env_int("DAL_SYNC_MAX_ROWS", default=0)
    if value is None or value <= 0:
        return 0
    return value


def cap_rows(rows: list, max_rows: int) -> list:
    """Return rows capped to max_rows (no-op when disabled)."""
    if max_rows and len(rows) > max_rows:
        return rows[:max_rows]
    return rows
