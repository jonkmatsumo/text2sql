"""Test that environment variables override defaults.

NOTE:
Renamed from test_config.py to avoid pytest import collisions with other
test_config.py modules in the monorepo when running tests from repo root.
"""

import importlib
import os
from unittest.mock import patch

from mcp_server.services.sanitization import text_sanitizer


def test_config_env_vars():
    """Test that environment variables override defaults."""
    # Reload with env vars set
    with patch.dict(os.environ, {"SANITIZER_MIN_LEN": "10", "SANITIZER_MAX_LEN": "20"}):
        importlib.reload(text_sanitizer)

        assert text_sanitizer.DEFAULT_MIN_LEN == 10
        assert text_sanitizer.DEFAULT_MAX_LEN == 20

        # Test effect on function defaults
        # Note: function default args are evaluated at definition time.
        # So sanitize_text defaults are bound when reloaded.

        # "short" is 5 chars. < 10. Should fail.
        res = text_sanitizer.sanitize_text("short")
        assert not res.is_valid
        assert "TOO_SHORT" in res.errors

    # Cleanup: Reload without env vars to restore defaults
    importlib.reload(text_sanitizer)
    assert text_sanitizer.DEFAULT_MIN_LEN == 2
