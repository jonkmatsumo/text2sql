import sys
import types
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Mock nest_asyncio and streamlit using ModuleType to avoid MagicMock pollution
# (e.g. pytest looking for setUpModule on the mock and getting a MagicMock)
mock_st = types.ModuleType("streamlit")
mock_st.sidebar = MagicMock()
mock_st.button = MagicMock()
mock_st.spinner = MagicMock()
# Handle Context Manager for spinner
mock_st.spinner.return_value.__enter__ = MagicMock()
mock_st.spinner.return_value.__exit__ = MagicMock()

# Needed for Admin_Panel import:
mock_st.set_page_config = MagicMock()
mock_st.title = MagicMock()
mock_st.header = MagicMock()
mock_st.subheader = MagicMock()
mock_st.divider = MagicMock()
mock_st.write = MagicMock()
mock_st.info = MagicMock()
mock_st.error = MagicMock()
mock_st.success = MagicMock()
mock_st.warning = MagicMock()
mock_st.columns = MagicMock(return_value=[MagicMock(), MagicMock(), MagicMock()])
mock_st.text_input = MagicMock()
mock_st.text_area = MagicMock()
mock_st.dataframe = MagicMock()
mock_st.code = MagicMock()
mock_st.json = MagicMock()
mock_st.rerun = MagicMock()
mock_st.session_state = MagicMock()
mock_st.status = MagicMock()
mock_st.metric = MagicMock()
mock_st.caption = MagicMock()

sys.modules["streamlit"] = mock_st

mock_nest = types.ModuleType("nest_asyncio")
mock_nest.apply = MagicMock()
sys.modules["nest_asyncio"] = mock_nest


@pytest.mark.asyncio
async def test_admin_panel_reload_button_logic():
    """Test verification of OpsService integration."""
    # Since we've mocked streamlit globally, importing Admin_Panel won't error out on st.* calls

    # Use MagicMock so it returns the dict immediately (since we bypass asyncio.run)
    # Correct import path from root where we run pytest
    with patch(
        "streamlit_app.service.ops_service.OpsService.reload_patterns", new_callable=MagicMock
    ) as mock_service_reload:
        mock_service_reload.return_value = {
            "success": True,
            "message": "Reloaded",
            "pattern_count": 10,
            "duration_ms": 123.4,
            "reload_id": "test-uuid",
        }

        # Setup st.button side effects to simulate user clicking "Reload NLP Patterns"
        def button_side_effect(label, **kwargs):
            if label == "Reload NLP Patterns":
                return True
            return False

        mock_st.button.side_effect = button_side_effect
        mock_st.sidebar.radio.return_value = "Operations"

        # Mocking asyncio.run to be identity so we don't need a real loop for the UI call check
        with patch("asyncio.run", side_effect=lambda x: x):
            import importlib.util

            # We import the module source to run it "as if" Streamlit ran it
            spec = importlib.util.spec_from_file_location(
                "Admin_Panel", "streamlit_app/pages/Admin_Panel.py"
            )
            val_mod = importlib.util.module_from_spec(spec)
            sys.modules["Admin_Panel"] = val_mod
            spec.loader.exec_module(val_mod)

            # Now run main
            val_mod.main()

            # Verify identity
            assert val_mod.st is mock_st, "Admin_Panel.st is not the mocked object!"

            # Verify button was queried
            calls = [c[0][0] for c in mock_st.button.call_args_list if c[0]]
            assert "Reload NLP Patterns" in calls, f"Button not clicked. Calls: {calls}"

            # Verify service was called
            mock_service_reload.assert_called_once()

            # Debug: Check if error was called
            if mock_st.error.called:
                args = mock_st.error.call_args
                pytest.fail(f"st.error was called with: {args}")

            # Verify success path
            if not mock_st.success.called:
                # Provide useful info on what WAS called in the MODULE
                # We can't use mock_st.mock_calls because mock_st is ModuleType
                # But we can check individual attributes we know of.
                log = []
                log.append(f"success: {mock_st.success.mock_calls}")
                log.append(f"error: {mock_st.error.mock_calls}")
                log.append(f"write: {mock_st.write.mock_calls}")
                log.append(f"info: {mock_st.info.mock_calls}")
                log.append(f"spinner: {mock_st.spinner.mock_calls}")

                # Also capture spinner returned context manager calls
                log.append(f"spinner_ctx: {mock_st.spinner.return_value.__enter__.mock_calls}")

                pytest.fail(f"st.success NOT called. Logs: {log}")

            mock_st.success.assert_called()
            mock_st.metric.assert_called_with("Patterns Loaded", 10)
            mock_st.error.assert_not_called()


@pytest.mark.asyncio
async def test_admin_ops_integration_sanity():
    """Sanity check that OpsService methods are exposed correctly for UI."""
    from streamlit_app.service.ops_service import OpsService

    # Ensure method exists
    assert hasattr(OpsService, "reload_patterns")

    # Verify call path works
    with patch.object(OpsService, "reload_patterns", new_callable=AsyncMock) as mock_reload:
        mock_reload.return_value = {"success": True}
        await OpsService.reload_patterns()
        mock_reload.assert_called_once()
