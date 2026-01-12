from mcp_server.services.sanitization.text_sanitizer import sanitize_text


def test_sanitize_valid_inputs():
    """Test valid inputs are accepted and normalized."""
    assert sanitize_text("hello world").is_valid
    assert sanitize_text("Testing 123").sanitized == "testing 123"
    assert sanitize_text("  spaces   here  ").sanitized == "spaces here"
    # Unicode canonicalization
    assert sanitize_text("café").sanitized == "café"


def test_sanitize_length_bounds():
    """Test min/max length checks."""
    # Min length (default 2)
    res = sanitize_text("a")
    assert not res.is_valid
    assert "TOO_SHORT" in res.errors

    # Max length
    long_str = "a" * 100
    res = sanitize_text(long_str)
    assert not res.is_valid
    assert "TOO_LONG" in res.errors


def test_sanitize_empty():
    """Test empty/whitespace inputs."""
    assert not sanitize_text("").is_valid
    assert not sanitize_text("   ").is_valid


def test_sanitize_regex_meta():
    """Test rejection of regex characters."""
    # Regex meta to reject: * ? | { } [ ] ^ $ \

    assert not sanitize_text("wildcard*").is_valid
    assert "CONTAINS_REGEX_META" in sanitize_text("wildcard*").errors

    assert not sanitize_text("regex[range]").is_valid
    assert not sanitize_text("pipe|char").is_valid
    assert not sanitize_text("back\\slash").is_valid


def test_sanitize_allowed_chars():
    """Test character allowlist."""
    # Allowed
    assert sanitize_text("user-name").is_valid
    assert sanitize_text("path/to/thing").is_valid
    assert sanitize_text("R&D").is_valid
    assert sanitize_text("O'Connor").is_valid
    assert sanitize_text("C++").is_valid
    assert sanitize_text("ver 1.0").is_valid
    assert sanitize_text("(parentheses)").is_valid

    # Not Allowed
    assert not sanitize_text("user@email").is_valid  # @ not in list
    assert not sanitize_text("hash#tag").is_valid  # # not in list
    assert not sanitize_text("emoji😊").is_valid


def test_sanitize_case_option():
    """Test lowercase option."""
    assert sanitize_text("Hello", lowercase=False).sanitized == "Hello"
    assert sanitize_text("Hello", lowercase=True).sanitized == "hello"
