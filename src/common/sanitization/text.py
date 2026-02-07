"""Generic text sanitization utility.

This module provides a domain-agnostic sanitizer for strings,
implementing strict normalization, length checks, and character allowlisting.
"""

import os
import re
import unicodedata
from dataclasses import dataclass
from typing import List, Optional

# Configuration Defaults
DEFAULT_MIN_LEN = int(os.getenv("SANITIZER_MIN_LEN", "2"))
DEFAULT_MAX_LEN = int(os.getenv("SANITIZER_MAX_LEN", "64"))

# Regex Metacharacters to explicitly reject
REGEX_META_CHARS = set("*?|{}[]^$\\")

# Allowed characters regex (conservative)
# Alphanumeric + space + common safe punctuation.
# - _ / & ' + . ( )
ALLOWED_CHARS_PATTERN = re.compile(r"^[a-zA-Z0-9\s\-_/&'+\.\(\)]+$")


@dataclass
class SanitizationResult:
    """Result of text sanitization."""

    sanitized: Optional[str]
    is_valid: bool
    errors: List[str]


def redact_sensitive_info(text: str) -> str:
    """Redact potentially sensitive information from strings.

    Targets:
    - Connection string credentials (e.g. postgresql://user:pass@host)
    - Bearer tokens
    - API keys (heuristic-based)
    """
    if not text:
        return text

    # Redact credentials in connection strings/URLs
    # Matches: protocol://user:pass@host
    res = re.sub(r"([a-zA-Z0-9+.-]+://)([^:/@]+):([^/@]+)@", r"\1<user>:<password>@", text)

    # Redact potential Bearer tokens
    res = re.sub(r"(?i)bearer\s+[a-zA-Z0-9._~+/-]+", "Bearer <redacted>", res)

    # Redact potential API keys (e.g. sk-..., key-...)
    res = re.sub(r"\b(sk-[a-zA-Z0-9]{20,})\b", "<api-key>", res)

    # Redact common sensitive key-value patterns
    # Matches: password=..., token: ..., etc.
    sensitive_keys = r"password|token|secret|api_key|auth|credential"
    res = re.sub(
        rf"(?i)\b({sensitive_keys})([ \t]*[=:][ \t]*)[^\s,;]+",
        r"\1\2<redacted>",
        res,
    )

    return res


def sanitize_text(
    text: str,
    min_len: int = DEFAULT_MIN_LEN,
    max_len: int = DEFAULT_MAX_LEN,
    lowercase: bool = True,
) -> SanitizationResult:
    """Sanitize and validate a raw string.

    Performs:
    1. Unicode Normalization (NFKC)
    2. Whitespace collapsing and trimming
    3. Case normalization (optional)
    4. Length checks
    5. Character allowlist check
    6. Explicit rejection of regex metacharacters

    Args:
        text: Raw input string.
        min_len: Minimum acceptable length.
        max_len: Maximum acceptable length.
        lowercase: Whether to lowercase the output.

    Returns:
        SanitizationResult object.
    """
    errors = []

    if not text:
        return SanitizationResult(None, False, ["EMPTY_INPUT"])

    # 1. Unicode Normalization
    normalized = unicodedata.normalize("NFKC", text)

    # 2. Whitespace Normalization (collapse internal spaces, trim ends)
    # Replace all whitespace sequences with single space
    cleaned = " ".join(normalized.split())

    # 3. Case Normalization
    if lowercase:
        cleaned = cleaned.lower()

    # Check for empty after trim
    if not cleaned:
        return SanitizationResult(None, False, ["EMPTY_AFTER_TRIM"])

    # 4. Length Checks
    if len(cleaned) < min_len:
        errors.append("TOO_SHORT")
    if len(cleaned) > max_len:
        errors.append("TOO_LONG")

    # 5. Regex Metacharacters Check (Fail fast on these)
    found_meta = [c for c in cleaned if c in REGEX_META_CHARS]
    if found_meta:
        errors.append("CONTAINS_REGEX_META")

    # 6. Character Allowlist Check
    if not ALLOWED_CHARS_PATTERN.match(cleaned):
        # We might want to be more specific about what failed, but for now just invalid chars
        # If we already flagged regex meta, this might be redundant but harmless
        if "CONTAINS_REGEX_META" not in errors:
            errors.append("INVALID_CHARACTERS")

    if errors:
        return SanitizationResult(cleaned, False, errors)

    return SanitizationResult(cleaned, True, [])
