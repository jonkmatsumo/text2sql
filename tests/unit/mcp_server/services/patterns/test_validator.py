from ingestion.patterns.validator import PatternValidator


def test_validator_basic_success():
    """Test valid patterns pass through."""
    validator = PatternValidator()
    patterns = [
        {"label": "RATING", "pattern": "G-rated", "id": "G"},
        {"label": "RATING", "pattern": "PG-rated", "id": "PG"},
    ]
    valid, failures = validator.validate_batch(patterns)

    assert len(valid) == 2
    assert len(failures) == 0
    assert valid[0]["pattern"] == "g-rated"  # Lowercased by sanitizer


def test_validator_sanitization_failures():
    """Test that sanitizer rejections are captured."""
    validator = PatternValidator()
    patterns = [
        {"label": "TEST", "pattern": "", "id": "1"},  # Empty
        {"label": "TEST", "pattern": "foo*", "id": "2"},  # Regex meta
        {"label": "TEST", "pattern": "valid", "id": "3"},
    ]
    valid, failures = validator.validate_batch(patterns)

    assert len(valid) == 1
    assert valid[0]["pattern"] == "valid"

    assert len(failures) == 2
    assert failures[0].reason == "SANITIZATION_FAILED"
    assert failures[1].raw_pattern == "foo*"


def test_validator_duplicates_in_batch():
    """Test duplicate detection within batch."""
    validator = PatternValidator()
    patterns = [
        {"label": "A", "pattern": "duplicate", "id": "1"},
        {"label": "A", "pattern": "Duplicate", "id": "1"},  # Same after normalization
        {"label": "A", "pattern": "unique", "id": "2"},
    ]
    valid, failures = validator.validate_batch(patterns)

    assert len(valid) == 2  # "duplicate" (first one) + "unique"
    assert len(failures) == 1
    assert failures[0].reason == "DUP_WITHIN_LABEL"
    assert failures[0].raw_pattern == "Duplicate"


def test_validator_cross_label_conflict_batch():
    """Test cross-label conflict in batch."""
    validator = PatternValidator()
    patterns = [
        {"label": "RATING", "pattern": "general", "id": "G"},
        {"label": "STATUS", "pattern": "general", "id": "COMMON"},  # Conflict
    ]
    valid, failures = validator.validate_batch(patterns)

    assert len(valid) == 1
    assert valid[0]["label"] == "RATING"

    assert len(failures) == 1
    assert failures[0].reason == "DUP_CROSS_LABEL"
    assert "Conflict with RATING" in failures[0].details


def test_validator_existing_conflict():
    """Test conflict with existing patterns."""
    validator = PatternValidator()
    existing = [{"label": "RATING", "pattern": "existing", "id": "G"}]
    patterns = [
        {"label": "STATUS", "pattern": "Existing", "id": "NEW"},  # Conflict
        {"label": "RATING", "pattern": "Existing", "id": "G"},  # Exact match (DUP_EXISTING_EXACT)
        {
            "label": "RATING",
            "pattern": "Existing",
            "id": "PG",
        },  # Ambiguous ID (DUP_EXISTING_CONFLICT)
    ]

    valid, failures = validator.validate_batch(patterns, existing_patterns=existing)

    assert len(valid) == 0
    assert len(failures) == 3

    reasons = [f.reason for f in failures]
    assert "DUP_EXISTING_CONFLICT" in reasons  # Status vs Rating
    assert "DUP_EXISTING_EXACT" in reasons  # Rating G vs Rating G

    # The third case: RATING 'Existing' ID 'PG' vs existing RATING 'Existing' ID 'G'.
    # This is "Ambiguous ID".
    # Since "Existing" maps to "G" in `known_patterns`.
    # Current input says "Existing" -> "PG".
    # Check logic:
    # if ex_label != label: (RATING == RATING) False
    # elif ex_id != pid: (G != PG) True -> DUP_EXISTING_CONFLICT "Ambiguous ID"

    assert reasons.count("DUP_EXISTING_CONFLICT") == 2
