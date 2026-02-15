from dal.schema_cache import SchemaCache, SchemaCacheBackend


class MockBackend(SchemaCacheBackend):
    """Mock backend for SchemaCache testing."""

    def __init__(self):
        """Initialize mock backend."""
        self.clear_calls = []

    def get(self, key):
        """Mock get method."""
        return None

    def set(self, key, value, ttl):
        """Mock set method."""
        pass

    def clear(self, provider=None, schema=None, table=None):
        """Mock clear method."""
        self.clear_calls.append((provider, schema, table))


def test_invalidate_calls_backend_clear():
    """Verify that invalidate methods propagate to the backend."""
    backend = MockBackend()
    cache = SchemaCache(backend=backend)

    # Test full invalidation
    cache.invalidate()
    assert backend.clear_calls[-1] == (None, None, None)

    # Test scoped invalidation
    cache.invalidate(provider="postgres", schema="public", table="users")
    assert backend.clear_calls[-1] == ("postgres", "public", "users")

    # Test provider-level invalidation
    cache.invalidate(provider="snowflake")
    assert backend.clear_calls[-1] == ("snowflake", None, None)
