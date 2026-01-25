"""Integration tests for Golden Dataset (Live MCP Server)."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add src to path
_repo_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_repo_root / "src"))

from agent.graph import run_agent_with_tracing  # noqa: E402


@pytest.fixture
def test_cases():
    """Fetch active test cases from database."""
    import sys
    from pathlib import Path

    # Add src to path to import the seed script
    repo_root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(repo_root / "src"))

    # Import INITIAL_TEST_CASES directly from the module
    import importlib.util

    seed_script_path = repo_root / "scripts" / "seed_golden_dataset.py"
    spec = importlib.util.spec_from_file_location("seed_golden_dataset", seed_script_path)
    seed_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(seed_module)
    INITIAL_TEST_CASES = seed_module.INITIAL_TEST_CASES

    # Return test cases (in production, fetch from database)
    return INITIAL_TEST_CASES


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.skip(reason="Requires running MCP server")
async def test_golden_dataset_easy_cases_integration(test_cases):
    """Test easy difficulty cases against live server."""
    with patch("agent.graph.telemetry") as mock_telemetry:
        mock_span = MagicMock()
        mock_telemetry.start_span.return_value.__enter__ = MagicMock(return_value=mock_span)
        mock_telemetry.start_span.return_value.__exit__ = MagicMock(return_value=False)
        mock_telemetry.capture_context.return_value = MagicMock()
        mock_telemetry.serialize_context.return_value = {}
        mock_telemetry.deserialize_context.return_value = MagicMock()
        mock_telemetry.use_context.return_value.__enter__ = MagicMock(return_value=None)
        mock_telemetry.use_context.return_value.__exit__ = MagicMock(return_value=False)
        mock_telemetry.configure = MagicMock()
        mock_telemetry.update_current_trace = MagicMock()
        mock_telemetry.get_current_trace_id.return_value = None
        mock_telemetry.get_current_span.return_value = None

        easy_cases = [tc for tc in test_cases if tc.get("difficulty") == "easy"]

        for test_case in easy_cases:
            result = await run_agent_with_tracing(
                question=test_case["question"],
                tenant_id=test_case.get("tenant_id", 1),
                session_id=f"integration-{test_case.get('category', 'unknown')}",
            )

            assert (
                result.get("current_sql") is not None
            ), f"Failed to generate SQL for: {test_case['question']}"
            assert (
                result.get("error") is None
            ), f"Execution error for: {test_case['question']} - {result.get('error')}"
            assert result.get("query_result") is not None, f"No result for: {test_case['question']}"


@pytest.mark.asyncio
@pytest.mark.integration
@pytest.mark.skip(reason="Requires running MCP server")
async def test_golden_dataset_aggregation_cases_integration(test_cases):
    """Test aggregation category cases against live server."""
    with patch("agent.graph.telemetry") as mock_telemetry:
        mock_span = MagicMock()
        mock_telemetry.start_span.return_value.__enter__ = MagicMock(return_value=mock_span)
        mock_telemetry.start_span.return_value.__exit__ = MagicMock(return_value=False)
        mock_telemetry.capture_context.return_value = MagicMock()
        mock_telemetry.serialize_context.return_value = {}
        mock_telemetry.deserialize_context.return_value = MagicMock()
        mock_telemetry.use_context.return_value.__enter__ = MagicMock(return_value=None)
        mock_telemetry.use_context.return_value.__exit__ = MagicMock(return_value=False)
        mock_telemetry.configure = MagicMock()
        mock_telemetry.update_current_trace = MagicMock()
        mock_telemetry.get_current_trace_id.return_value = None
        mock_telemetry.get_current_span.return_value = None

        agg_cases = [tc for tc in test_cases if "aggregation" in tc.get("category", "")]

        for test_case in agg_cases:
            result = await run_agent_with_tracing(
                question=test_case["question"],
                tenant_id=test_case.get("tenant_id", 1),
                session_id="integration-aggregation",
            )

            assert result.get("current_sql") is not None
            assert result.get("error") is None
            assert result.get("query_result") is not None
