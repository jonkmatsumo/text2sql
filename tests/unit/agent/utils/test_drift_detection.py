"""Tests for AST-based schema drift detection."""

import pytest

from agent.utils.drift_detection import detect_schema_drift, detect_schema_drift_details
from common.constants.reason_codes import DriftDetectionMethod

_DRIFT_CONTRACT_KEYS = {
    "missing_identifiers",
    "method",
    "source",
    "last_error_code",
    "error_code",
    "error_message",
    "reference_resolution_mode",
    "resolution_mode",
    "reference_model_version",
    "reference_available",
    "reference_selection_source",
    "bucketing_requested",
    "bucketing_used",
}


def _assert_drift_contract_shape(payload: dict) -> None:
    assert set(payload.keys()) == _DRIFT_CONTRACT_KEYS
    assert payload["last_error_code"] == payload["error_code"]
    assert payload["reference_resolution_mode"] == payload["resolution_mode"]
    assert isinstance(payload["reference_available"], bool)
    assert payload["reference_selection_source"] in {"alias", "stage", "latest", "none"}
    if payload["error_message"] is not None:
        assert len(payload["error_message"]) <= 200


@pytest.fixture
def raw_schema_context():
    """Return a sample raw schema context for testing."""
    return [
        {"type": "Table", "name": "users"},
        {"type": "Column", "table": "users", "name": "id"},
        {"type": "Column", "table": "users", "name": "name"},
        {"type": "Table", "name": "orders"},
        {"type": "Column", "table": "orders", "name": "id"},
        {"type": "Column", "table": "orders", "name": "user_id"},
    ]


def test_detect_schema_drift_no_drift(raw_schema_context):
    """Test that no drift is detected when all identifiers are present."""
    sql = "SELECT name FROM users WHERE id = 1"
    error_message = ""
    provider = "postgres"

    missing, method = detect_schema_drift(sql, error_message, provider, raw_schema_context)

    assert missing == []
    assert method == DriftDetectionMethod.AST


def test_detect_schema_drift_missing_table(raw_schema_context):
    """Test that a missing table is detected via AST."""
    sql = "SELECT * FROM products"
    # Even if error message doesn't say it, AST should find it if it's not in schema context
    error_message = ""
    provider = "postgres"

    missing, method = detect_schema_drift(sql, error_message, provider, raw_schema_context)

    assert "products" in missing
    assert method == DriftDetectionMethod.AST


def test_detect_schema_drift_missing_column(raw_schema_context):
    """Test that a missing column is detected via AST."""
    sql = "SELECT email FROM users"
    error_message = ""
    provider = "postgres"

    missing, method = detect_schema_drift(sql, error_message, provider, raw_schema_context)

    assert "email" in missing
    assert method == DriftDetectionMethod.AST


def test_detect_schema_drift_regex_fallback():
    """Test fallback to regex detection when SQL parsing fails."""
    sql = "INVALID SQL !!!"
    # Regex should catch it from error message
    error_message = 'relation "missing_table" does not exist'
    provider = "postgres"

    missing, method = detect_schema_drift(sql, error_message, provider, [])

    assert "missing_table" in missing
    assert method == DriftDetectionMethod.REGEX_FALLBACK


def test_detect_schema_drift_hybrid(raw_schema_context):
    """Test hybrid detection using both AST and regex."""
    sql = "SELECT name FROM users JOIN products ON users.id = products.user_id"
    # AST finds 'products' and 'products.user_id'
    # Regex finds 'products'
    error_message = 'relation "products" does not exist'
    provider = "postgres"

    missing, method = detect_schema_drift(sql, error_message, provider, raw_schema_context)

    assert "products" in missing
    # Note: 'products.user_id' is NOT included because 'products' table is already missing
    assert method == DriftDetectionMethod.HYBRID


def test_detect_schema_drift_with_catalog(raw_schema_context):
    """Test that qualified table names with catalogs are handled."""
    # We add a catalog to the schema context mock for this test
    context = raw_schema_context + [
        {"type": "Table", "name": "analytics.sales.events"},
        {"type": "Column", "table": "analytics.sales.events", "name": "id"},
    ]

    # Matching
    sql = "SELECT id FROM analytics.sales.events"
    missing, _ = detect_schema_drift(sql, "", "postgres", context)
    assert "analytics.sales.events" not in missing
    assert "id" not in missing

    # Mismatching catalog
    sql = "SELECT id FROM wrong.sales.events"
    missing, _ = detect_schema_drift(sql, "", "postgres", context)
    assert "wrong.sales.events" in missing


def test_detect_schema_drift_no_context_regex_only():
    """Test that when schema context is missing, it relies on regex even if SQL is valid."""
    sql = "SELECT * FROM users"
    error_message = 'relation "users" does not exist'

    missing, method = detect_schema_drift(sql, error_message, "postgres", [])

    assert "users" in missing
    # Since AST couldn't validate anything (no context), it should be REGEX_FALLBACK or HYBRID?
    # Currently it would be HYBRID because ast exists but missing_identifiers from AST is empty.
    # Wait, if ast exists but missing_identifiers is empty, and regex finds something, it is HYBRID.
    assert method == DriftDetectionMethod.HYBRID


def test_detect_schema_drift_prefers_structured_signal(raw_schema_context):
    """Structured SQLSTATE indicators should be preferred over regex parsing."""
    sql = "SELECT email FROM users"
    error_message = 'relation "users" does not exist'
    error_metadata = {"sql_state": "42703", "code": "42703"}

    result = detect_schema_drift_details(
        sql=sql,
        error_message=error_message,
        provider="postgres",
        raw_schema_context=raw_schema_context,
        error_metadata=error_metadata,
    )

    assert "email" in result.missing_identifiers
    assert result.source == "structured"
    assert result.method == DriftDetectionMethod.HYBRID


def test_detect_schema_drift_uses_regex_source_without_structured_metadata():
    """Regex fallback should be the source when no structured signal exists."""
    sql = "INVALID SQL !!!"
    error_message = 'relation "missing_table" does not exist'

    result = detect_schema_drift_details(
        sql=sql,
        error_message=error_message,
        provider="postgres",
        raw_schema_context=[],
        error_metadata=None,
    )

    assert "missing_table" in result.missing_identifiers
    assert result.source == "regex"
    assert result.method == DriftDetectionMethod.REGEX_FALLBACK


def test_drift_contract_no_reference_model_available():
    """Drift contract should report bounded metadata when reference context is unavailable."""
    result = detect_schema_drift_details(
        sql="SELECT * FROM users",
        error_message="Reference model unavailable for drift check",
        provider="postgres",
        raw_schema_context=[],
        error_metadata=None,
    )

    assert result.error_code == "no_reference_model"
    assert result.error_message == "Reference model unavailable for drift check"
    assert len(result.error_message) <= 200
    assert result.resolution_mode == "none"
    assert result.reference_model_version is None
    assert result.bucketing_requested is None
    assert result.bucketing_used is None
    payload = result.to_dict()
    _assert_drift_contract_shape(payload)
    assert payload["reference_available"] is False
    assert payload["reference_selection_source"] == "none"


def test_drift_contract_insufficient_reference_samples_metadata():
    """Explicit insufficient-sample metadata should map to canonical drift fields."""
    result = detect_schema_drift_details(
        sql="SELECT email FROM users",
        error_message="Reference sample count below minimum",
        provider="postgres",
        raw_schema_context=[{"type": "Table", "name": "users"}],
        error_metadata={
            "drift_error_code": "insufficient_reference_samples",
            "drift_error_message": "Need >=100 reference rows for drift estimation",
            "reference_resolution_mode": "stage",
            "reference_model_version": "model-v7",
            "bucketing_requested": True,
            "bucketing_used": False,
        },
    )

    assert result.error_code == "insufficient_reference_samples"
    assert result.error_message == "Need >=100 reference rows for drift estimation"
    assert result.resolution_mode == "stage"
    assert result.reference_model_version == "model-v7"
    assert result.bucketing_requested is True
    assert result.bucketing_used is False
    payload = result.to_dict()
    _assert_drift_contract_shape(payload)
    assert payload["reference_available"] is True
    assert payload["reference_selection_source"] == "stage"


def test_drift_contract_sparse_bucket_psi_suppression_maps_to_canonical_code():
    """PSI sparse-bucket suppression should map to a stable canonical error code."""
    result = detect_schema_drift_details(
        sql="SELECT id FROM users",
        error_message="PSI suppressed due to sparse buckets in reference histogram",
        provider="postgres",
        raw_schema_context=[{"type": "Table", "name": "users"}],
        error_metadata={"resolution_mode": "alias"},
    )

    assert result.error_code == "psi_sparse_buckets"
    assert result.error_message == "PSI suppressed due to sparse buckets in reference histogram"
    assert result.resolution_mode == "alias"
    payload = result.to_dict()
    _assert_drift_contract_shape(payload)
    assert payload["reference_selection_source"] == "alias"


def test_drift_contract_success_path_keeps_error_fields_empty(raw_schema_context):
    """Successful drift detection should keep standardized error fields empty."""
    result = detect_schema_drift_details(
        sql="SELECT name FROM users",
        error_message="",
        provider="postgres",
        raw_schema_context=raw_schema_context,
        error_metadata=None,
    )

    assert result.error_code is None
    assert result.error_message is None
    assert result.resolution_mode == "latest"
    payload = result.to_dict()
    _assert_drift_contract_shape(payload)
    assert payload["reference_available"] is True
    assert payload["reference_selection_source"] == "latest"
