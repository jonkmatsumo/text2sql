"""Tests for typed environment variable parsing helpers."""

import os
from unittest.mock import patch

import pytest

from common.config.env import get_env_bool, get_env_float, get_env_int, get_env_list, get_env_str


def test_get_env_str():
    """Test string parsing."""
    with patch.dict(os.environ, {"FOO": "bar"}):
        assert get_env_str("FOO") == "bar"
        assert get_env_str("BAR", default="baz") == "baz"
        assert get_env_str("FOO", required=True) == "bar"

    with pytest.raises(KeyError):
        get_env_str("MISSING", required=True)


def test_get_env_int():
    """Test integer parsing."""
    with patch.dict(os.environ, {"FOO": "123"}):
        assert get_env_int("FOO") == 123
        assert get_env_int("BAR", default=456) == 456

    with patch.dict(os.environ, {"FOO": "not_an_int"}):
        with pytest.raises(ValueError):
            get_env_int("FOO")


def test_get_env_float():
    """Test float parsing."""
    with patch.dict(os.environ, {"FOO": "1.23"}):
        assert get_env_float("FOO") == 1.23
        assert get_env_float("BAR", default=4.56) == 4.56

    with patch.dict(os.environ, {"FOO": "not_a_float"}):
        with pytest.raises(ValueError):
            get_env_float("FOO")


def test_get_env_bool():
    """Test boolean parsing."""
    truthy = ["true", "1", "yes", "on", "TRUE", "Yes"]
    falsey = ["false", "0", "no", "off", "", "FALSE", "No"]

    for val in truthy:
        with patch.dict(os.environ, {"FOO": val}):
            assert get_env_bool("FOO") is True

    for val in falsey:
        with patch.dict(os.environ, {"FOO": val}):
            assert get_env_bool("FOO") is False

    assert get_env_bool("MISSING", default=True) is True

    with patch.dict(os.environ, {"FOO": "maybe"}):
        with pytest.raises(ValueError):
            get_env_bool("FOO")


def test_get_env_list():
    """Test list parsing."""
    with patch.dict(os.environ, {"FOO": "a, b,  c "}):
        assert get_env_list("FOO") == ["a", "b", "c"]

    with patch.dict(os.environ, {"FOO": "a;b;c"}):
        assert get_env_list("FOO", separator=";") == ["a", "b", "c"]

    assert get_env_list("MISSING", default=["x"]) == ["x"]

    with patch.dict(os.environ, {"FOO": ", , , "}):
        assert get_env_list("FOO") == []
