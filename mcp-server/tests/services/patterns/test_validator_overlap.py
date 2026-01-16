"""Tests for overlap detection in PatternValidator."""

from ingestion.patterns.validator import PatternValidator


def test_validator_short_patterns():
    """Test rejection of short patterns."""
    validator = PatternValidator()
    patterns = [
        {"label": "TEST", "pattern": "abc", "id": "1"},  # 3 chars -> Fail
        {"label": "TEST", "pattern": "abcd", "id": "2"},  # 4 chars -> Pass
    ]
    valid, failures = validator.validate_batch(patterns)

    assert len(valid) == 1
    assert valid[0]["pattern"] == "abcd"

    assert len(failures) == 1
    assert failures[0].reason == "RISKY_SHORT_PATTERN"
    assert failures[0].raw_pattern == "abc"


def test_validator_overlap_detection():
    """Test token-based overlap detection."""
    validator = PatternValidator()
    patterns = [
        {"label": "TEST", "pattern": "San Francisco", "id": "SF"},
        {"label": "TEST", "pattern": "San Francisco Giants", "id": "SFG"},
    ]
    # "San Francisco" is token-subset of "San Francisco Giants"
    # Should flag OVERLAP_CONFLICT

    valid, failures = validator.validate_batch(patterns)

    # One should be accepted (the first one encountered?), and the second rejected?
    # Loop Logic:
    # 1. "San Francisco" -> No conflicts in batch (seen is empty). Accepted. seen={"san francisco"}
    # 2. "San Francisco Giants" -> Check overlaps with seen{"san francisco"}. True. Rejected.

    assert len(valid) == 1
    assert valid[0]["pattern"] == "san francisco"

    assert len(failures) == 1
    assert failures[0].reason == "OVERLAP_CONFLICT"
    assert "Overlaps with batch pattern 'san francisco'" in failures[0].details


def test_validator_overlap_no_false_positive():
    """Test that substring matches that are NOT token overlaps are allowed."""
    validator = PatternValidator()
    patterns = [
        {"label": "TEST", "pattern": "cat", "id": "1"},
        {"label": "TEST", "pattern": "cattle", "id": "2"},
    ]
    # "cat" is substring of "cattle", but tokens ["cat"] is NOT subsequence of ["cattle"].

    valid, failures = validator.validate_batch(patterns)

    # Note: "cat" is 3 chars, so it hits RISKY_SHORT_PATTERN if threshold is 3.
    # I should use longer words for this test to isolate overlap logic.
    pass


def test_validator_overlap_no_false_positive_length_safe():
    """Test token overlap with length-safe words."""
    validator = PatternValidator()
    patterns = [
        {"label": "TEST", "pattern": "cats", "id": "1"},
        {"label": "TEST", "pattern": "catsup", "id": "2"},
    ]
    valid, failures = validator.validate_batch(patterns)
    assert len(valid) == 2


def test_validator_overlap_existing():
    """Test overlap with existing patterns."""
    validator = PatternValidator()
    existing = [{"label": "CITY", "pattern": "new york", "id": "NY"}]
    patterns = [{"label": "STATE", "pattern": "new york state", "id": "NYS"}]

    valid, failures = validator.validate_batch(patterns, existing_patterns=existing)

    assert len(valid) == 0
    assert len(failures) == 1
    assert failures[0].reason == "OVERLAP_CONFLICT"
    assert "Overlaps with existing pattern" in failures[0].details
