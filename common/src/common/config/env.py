"""Typed environment variable parsing helpers."""

import os
from typing import List, Optional, TypeVar

T = TypeVar("T")


def get_env_str(name: str, default: Optional[str] = None, required: bool = False) -> Optional[str]:
    """Get an environment variable as a string."""
    value = os.getenv(name)
    if value is None:
        if required:
            raise KeyError(f"Environment variable '{name}' is required but not set.")
        return default
    return value


def get_env_int(name: str, default: Optional[int] = None, required: bool = False) -> Optional[int]:
    """Get an environment variable as an integer."""
    value = os.getenv(name)
    if value is None:
        if required:
            raise KeyError(f"Environment variable '{name}' is required but not set.")
        return default
    try:
        return int(value)
    except ValueError:
        raise ValueError(f"Environment variable '{name}' must be an integer, got '{value}'.")


def get_env_float(
    name: str, default: Optional[float] = None, required: bool = False
) -> Optional[float]:
    """Get an environment variable as a float."""
    value = os.getenv(name)
    if value is None:
        if required:
            raise KeyError(f"Environment variable '{name}' is required but not set.")
        return default
    try:
        return float(value)
    except ValueError:
        raise ValueError(f"Environment variable '{name}' must be a float, got '{value}'.")


def get_env_bool(
    name: str, default: Optional[bool] = None, required: bool = False
) -> Optional[bool]:
    """Get an environment variable as a boolean.

    Truthy: true, 1, yes, on
    Falsey: false, 0, no, off, (empty string)
    """
    value = os.getenv(name)
    if value is None:
        if required:
            raise KeyError(f"Environment variable '{name}' is required but not set.")
        return default

    val_lower = value.lower()
    if val_lower in ("true", "1", "yes", "on"):
        return True
    if val_lower in ("false", "0", "no", "off", ""):
        return False

    raise ValueError(f"Environment variable '{name}' must be a boolean, got '{value}'.")


def get_env_list(
    name: str, default: Optional[List[str]] = None, required: bool = False, separator: str = ","
) -> Optional[List[str]]:
    """Get an environment variable as a list of strings."""
    value = os.getenv(name)
    if value is None:
        if required:
            raise KeyError(f"Environment variable '{name}' is required but not set.")
        return default

    return [s.strip() for s in value.split(separator) if s.strip()]
