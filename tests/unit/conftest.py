"""Unit test environment helpers."""

import os

import pytest


@pytest.fixture(autouse=True)
def _minimal_env(monkeypatch):
    """Set minimal env defaults for unit tests without external deps."""
    if not os.getenv("QUERY_TARGET_BACKEND") and not os.getenv("QUERY_TARGET_PROVIDER"):
        monkeypatch.setenv("QUERY_TARGET_BACKEND", "postgres")
    key = os.getenv("OPENAI_API_KEY")
    placeholders = {"<REPLACE_ME>", "changeme", "your_api_key_here"}
    if not key or key.strip() in placeholders or key.startswith("<"):
        monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("LLM_MODEL", "gpt-4o")
    monkeypatch.setenv("SCHEMA_SNAPSHOT_MODE", "fingerprint")
    monkeypatch.delenv("POSTGRES_URL", raising=False)
    yield


@pytest.fixture(autouse=True)
def _reset_database_state():
    """Reset global Database state after each test."""
    from dal.database import Database

    # Snapshot current state
    original_pool = Database._pool
    original_provider = Database._query_target_provider
    original_capabilities = Database._query_target_capabilities
    original_sync_max_rows = Database._query_target_sync_max_rows
    original_graph = Database._graph_store
    original_cache = Database._cache_store
    original_example = Database._example_store
    original_schema = Database._schema_store
    original_introspector = Database._schema_introspector
    original_metadata = Database._metadata_store

    yield

    # Restore state
    Database._pool = original_pool
    Database._query_target_provider = original_provider
    Database._query_target_capabilities = original_capabilities
    Database._query_target_sync_max_rows = original_sync_max_rows
    Database._graph_store = original_graph
    Database._cache_store = original_cache
    Database._example_store = original_example
    Database._schema_store = original_schema
    Database._schema_introspector = original_introspector
    Database._metadata_store = original_metadata
