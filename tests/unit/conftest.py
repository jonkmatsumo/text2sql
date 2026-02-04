"""Unit test environment helpers."""

import os

import pytest


@pytest.fixture(autouse=True)
def _minimal_env(monkeypatch):
    """Set minimal env defaults for unit tests without external deps."""
    if not os.getenv("QUERY_TARGET_BACKEND") and not os.getenv("QUERY_TARGET_PROVIDER"):
        monkeypatch.setenv("QUERY_TARGET_BACKEND", "postgres")
    monkeypatch.setenv("LLM_MODEL", "gpt-4o")
    monkeypatch.setenv("SCHEMA_SNAPSHOT_MODE", "fingerprint")
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    yield
