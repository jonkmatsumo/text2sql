"""Tests for Grafana dashboard JSON files.

Validates that dashboard JSON files are parseable and contain required Grafana fields.
"""

import json
from pathlib import Path

import pytest

DASHBOARDS_DIR = Path(__file__).parent.parent.parent / "grafana" / "dashboards"

REQUIRED_TOP_LEVEL_FIELDS = {"uid", "title", "panels", "schemaVersion"}


def get_dashboard_files() -> list[Path]:
    """Get all JSON files in the dashboards directory."""
    return list(DASHBOARDS_DIR.glob("*.json"))


@pytest.fixture(params=get_dashboard_files(), ids=lambda p: p.name)
def dashboard_file(request) -> Path:
    """Parametrized fixture for each dashboard JSON file."""
    return request.param


@pytest.fixture
def dashboard_json(dashboard_file: Path) -> dict:
    """Load and parse dashboard JSON."""
    with open(dashboard_file) as f:
        return json.load(f)


class TestDashboardJsonValidity:
    """Test that all dashboards are valid JSON with required fields."""

    def test_json_parses_successfully(self, dashboard_file: Path):
        """Verify dashboard JSON files parse without error."""
        with open(dashboard_file) as f:
            data = json.load(f)
        assert isinstance(data, dict)

    def test_required_top_level_fields_exist(self, dashboard_json: dict, dashboard_file: Path):
        """Verify required Grafana dashboard fields are present."""
        missing = REQUIRED_TOP_LEVEL_FIELDS - set(dashboard_json.keys())
        assert not missing, f"{dashboard_file.name} missing fields: {missing}"

    def test_uid_is_non_empty_string(self, dashboard_json: dict, dashboard_file: Path):
        """Verify uid is a non-empty string."""
        uid = dashboard_json.get("uid")
        assert isinstance(uid, str) and uid, f"{dashboard_file.name} has invalid uid"

    def test_title_is_non_empty_string(self, dashboard_json: dict, dashboard_file: Path):
        """Verify title is a non-empty string."""
        title = dashboard_json.get("title")
        assert isinstance(title, str) and title, f"{dashboard_file.name} has invalid title"

    def test_panels_is_list(self, dashboard_json: dict, dashboard_file: Path):
        """Verify panels is a list."""
        panels = dashboard_json.get("panels")
        assert isinstance(panels, list), f"{dashboard_file.name} panels is not a list"


class TestTraceDetailDashboard:
    """Specific tests for the trace detail dashboard."""

    @pytest.fixture
    def trace_detail_json(self) -> dict:
        """Load trace_detail.json specifically."""
        path = DASHBOARDS_DIR / "trace_detail.json"
        if not path.exists():
            pytest.skip("trace_detail.json not yet created")
        with open(path) as f:
            return json.load(f)

    def test_has_trace_id_variable(self, trace_detail_json: dict):
        """Verify trace_id variable is defined in templating."""
        templating = trace_detail_json.get("templating", {})
        variables = templating.get("list", [])
        variable_names = [v.get("name") for v in variables]
        assert "trace_id" in variable_names, "trace_detail.json must have trace_id variable"

    def test_trace_id_variable_is_textbox(self, trace_detail_json: dict):
        """Verify trace_id variable is a textbox type."""
        templating = trace_detail_json.get("templating", {})
        variables = templating.get("list", [])
        trace_id_vars = [v for v in variables if v.get("name") == "trace_id"]
        assert trace_id_vars, "trace_id variable not found"
        assert trace_id_vars[0].get("type") == "textbox", "trace_id should be textbox type"

    def test_uid_matches_expected(self, trace_detail_json: dict):
        """Verify dashboard uid is correct for linking."""
        assert trace_detail_json.get("uid") == "text2sql-trace-detail"
