"""Integration tests for generator pipeline with validation."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Import the module under test to ensure patching works on it
from ingestion.patterns import generator
from schema import ColumnDef, TableDef


@pytest.mark.asyncio
async def test_generator_validation_integration():
    """Verify that generate_entity_patterns validates and filters LLM output."""
    # Mock Introspector
    mock_introspector = AsyncMock()
    mock_introspector.list_table_names.return_value = ["test_table"]

    mock_table_def = TableDef(
        name="test_table", columns=[ColumnDef(name="status", data_type="text", is_nullable=True)]
    )
    mock_introspector.get_table_def.return_value = mock_table_def

    # Mock OpenAI
    mock_client = AsyncMock()
    mock_response = MagicMock()

    # Bad payload
    bad_synonyms = [
        {"pattern": "  ", "id": "TEST"},  # Bad Sanitization
        {"pattern": "valid synonym", "id": "TEST"},  # Good
        {"pattern": ".*", "id": "TEST"},  # Regex Meta
        {"pattern": "abc", "id": "TEST"},  # Short (<=3) if policy is active
    ]
    mock_response.choices[0].message.content = json.dumps(bad_synonyms)
    mock_client.chat.completions.create.return_value = mock_response

    # Helper to mock async context manager
    mock_conn = AsyncMock()

    # Patch dependencies
    with (
        patch("ingestion.patterns.generator.Database") as MockDB,
        patch("ingestion.patterns.generator.get_openai_client", return_value=mock_client),
        patch("ingestion.patterns.generator.EnumLikeColumnDetector") as MockDetectorCls,
        patch("ingestion.patterns.generator.sample_distinct_values", return_value=["ACTIVE"]),
        patch("ingestion.patterns.generator.get_native_enum_values", return_value=[]),
    ):

        MockDB.get_schema_introspector.return_value = mock_introspector
        MockDB.get_connection.return_value.__aenter__.return_value = mock_conn

        # Mock Detector
        mock_detector = MockDetectorCls.return_value
        mock_detector.threshold = 10
        mock_detector.is_candidate.return_value = True
        mock_detector.canonicalize_values.return_value = ["ACTIVE"]

        # Run Generator
        patterns = await generator.generate_entity_patterns()

        # Extract patterns strings
        final_patterns = [p["pattern"] for p in patterns]

        # 1. Check Valid Synonym is present
        assert "valid synonym" in final_patterns

        # 2. Check Invalid are stripped
        assert "  " not in final_patterns
        assert "" not in final_patterns
        assert ".*" not in final_patterns

        # 3. Check Short pattern rejection (if enabled)
        # "abc" is 3 chars. Logic: len <= 3 -> RISKY_SHORT_PATTERN -> Fail
        assert "abc" not in final_patterns

        # 4. Check Standard Scan patterns
        assert "test table" in final_patterns
        assert "status" in final_patterns  # Column name as pattern?
        # "status" might be rejected if min_len > 6 ? No, "status" is 6 chars.
        # Wait, sanitize defaults: min=2.
        # So "status" is fine.

        assert "active" in final_patterns  # The value itself "ACTIVE" -> "active"
