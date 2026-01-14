"""Test that environment variables override defaults for common sanitization."""

import importlib
import os
from unittest.mock import patch

from common.sanitization import text


def test_config_env_vars():
    """Test that environment variables override defaults."""
    # Reload with env vars set
    with patch.dict(os.environ, {"SANITIZER_MIN_LEN": "10", "SANITIZER_MAX_LEN": "20"}):
        importlib.reload(text)

        assert text.DEFAULT_MIN_LEN == 10
        assert text.DEFAULT_MAX_LEN == 20

        # Test effect on function defaults
        # Note: function default args are evaluated at definition time.
        # So sanitize_text defaults are bound when reloaded.
        res = text.sanitize_text("short")
        assert not res.is_valid
        assert "TOO_SHORT" in res.errors

    # Cleanup: Reload without env vars to restore defaults
    importlib.reload(text)
    assert text.DEFAULT_MIN_LEN == 2
