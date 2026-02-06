from common.sanitization.text import redact_sensitive_info


def test_redact_sensitive_info():
    """Test that sensitive information is correctly redacted."""
    assert (
        redact_sensitive_info("postgresql://user:pass@localhost:5432/db")
        == "postgresql://<user>:<password>@localhost:5432/db"
    )
    assert redact_sensitive_info("Bearer my-token-123") == "Bearer <redacted>"
    assert redact_sensitive_info("api key is sk-12345678901234567890") == "api key is <api-key>"
    assert redact_sensitive_info("normal text") == "normal text"
    assert redact_sensitive_info("") == ""
    assert redact_sensitive_info(None) is None


if __name__ == "__main__":
    test_redact_sensitive_info()
    print("✓ Sanitization tests passed")
