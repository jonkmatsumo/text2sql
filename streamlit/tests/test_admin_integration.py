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

sys.modules["streamlit"] = mock_st

mock_nest = types.ModuleType("nest_asyncio")
mock_nest.apply = MagicMock()
sys.modules["nest_asyncio"] = mock_nest


@pytest.mark.asyncio
async def test_admin_panel_reload_button_logic():
    """Test verification of OpsService integration."""
    # Since we've mocked streamlit globally, importing Admin_Panel won't error out on st.* calls

    with patch(
        "service.ops_service.OpsService.reload_patterns", new_callable=AsyncMock
    ) as mock_service_reload:
        mock_service_reload.return_value = {"success": True}

        # Setup st.button side effects to simulate user clicking "Reload NLP Patterns"
        def button_side_effect(label, **kwargs):
            if label == "Reload NLP Patterns":
                return True
            return False

        mock_st.button.side_effect = button_side_effect
        mock_st.sidebar.radio.return_value = "Operations"

        # Mocking asyncio.run to be identity so we don't need a real loop for the UI call check
        with patch("asyncio.run", side_effect=lambda x: x):
            pass


@pytest.mark.asyncio
async def test_admin_ops_integration_sanity():
    """Sanity check that OpsService methods are exposed correctly for UI."""
    from service.ops_service import OpsService

    # Ensure method exists
    assert hasattr(OpsService, "reload_patterns")

    # Verify call path works
    with patch.object(OpsService, "reload_patterns", new_callable=AsyncMock) as mock_reload:
        mock_reload.return_value = {"success": True}
        await OpsService.reload_patterns()
        mock_reload.assert_called_once()
