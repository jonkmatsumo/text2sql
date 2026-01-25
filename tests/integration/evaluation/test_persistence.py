import json
from pathlib import Path

# Mock Schema
SCHEMA_PATH = Path("config/services/evaluation/metrics_v1.json")


def test_metrics_schema_valid():
    """Ensure the schema file is valid JSON."""
    assert SCHEMA_PATH.exists()
    schema = json.loads(SCHEMA_PATH.read_text())
    assert schema["title"] == "EvaluationMetricsV1"
    assert "exact_match_rate" in schema["required"]
