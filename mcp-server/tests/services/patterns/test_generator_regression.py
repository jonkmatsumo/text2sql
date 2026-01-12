import json
from unittest.mock import AsyncMock, MagicMock

import pytest
from mcp_server.services.patterns.generator import enrich_values_with_llm


@pytest.mark.asyncio
async def test_enrich_values_persists_unsafe_patterns():
    """Verify that currently the generator accepts and returns malformed patterns.

    This confirmation of the regression demonstrates the need for the sanitizer/validator.
    """
    # Mock OpenAI client
    mock_client = AsyncMock()
    mock_response = MagicMock()

    # Malicious/Bad Payload that mimics a confused or malicious LLM
    long_string = "x" * 1000

    bad_synonyms = [
        {"pattern": "  ", "id": "TEST"},  # Whitespace
        {"pattern": ".*", "id": "TEST"},  # Regex meta-char
        {"pattern": "DROP TABLE users", "id": "TEST"},  # Unsafe/Injection-like
        {"pattern": "duplicate", "id": "TEST"},  # Duplicate 1
        {"pattern": "duplicate", "id": "TEST"},  # Duplicate 2 (Identical)
        {"pattern": "a", "id": "TEST"},  # Too short
        {"pattern": long_string, "id": "TEST"},  # Too long
        {"pattern": "valid synonym", "id": "TEST"},  # One good one
    ]

    # Setup mock to return this JSON
    mock_response.choices[0].message.content = json.dumps(bad_synonyms)
    mock_client.chat.completions.create.return_value = mock_response

    label = "TEST_LABEL"
    values = ["TEST"]

    # Execute
    results = await enrich_values_with_llm(mock_client, label, values)

    # Extract patterns
    patterns = [r["pattern"] for r in results]

    # Assertions - verifies that the system CURRENTLY fails to catch these.
    # We WANT these to eventually fail these assertions (when fixed),
    # but for now we assert they are present to prove the regression/vulnerability.

    # 1. Whitespace preserved (just lowercased)
    assert "  " in patterns

    # 2. Regex chars preserved
    assert ".*" in patterns

    # 3. SQL injection preserved
    assert "drop table users" in patterns

    # 4. Duplicates preserved
    assert patterns.count("duplicate") == 2

    # 5. Length checks missing
    assert "a" in patterns
    assert long_string in patterns
