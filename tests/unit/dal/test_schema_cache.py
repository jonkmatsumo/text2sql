import pytest

from dal.schema_cache import CachedSchemaIntrospector, SchemaCache


class _FakeIntrospector:
    def __init__(self):
        self.list_calls = 0
        self.def_calls = 0

    async def list_table_names(self, schema="public"):
        self.list_calls += 1
        return ["users"]

    async def get_table_def(self, table_name, schema="public"):
        self.def_calls += 1
        return {"name": table_name}

    async def get_sample_rows(self, table_name, limit=3, schema="public"):
        return [{"id": 1}]


@pytest.mark.asyncio
async def test_schema_cache_hit_miss(monkeypatch):
    """Cache returns cached values after first miss."""
    cache = SchemaCache(ttl_seconds=60)
    wrapped = _FakeIntrospector()
    cached = CachedSchemaIntrospector("postgres", wrapped, cache)

    first = await cached.list_table_names(schema="public")
    second = await cached.list_table_names(schema="public")

    assert first == ["users"]
    assert second == ["users"]
    assert wrapped.list_calls == 1


@pytest.mark.asyncio
async def test_schema_cache_ttl_expiration(monkeypatch):
    """Cache expires entries after TTL."""
    cache = SchemaCache(ttl_seconds=10)
    wrapped = _FakeIntrospector()
    cached = CachedSchemaIntrospector("postgres", wrapped, cache)

    current = {"now": 0}

    def fake_time():
        return current["now"]

    monkeypatch.setattr("dal.schema_cache.time.time", fake_time)

    await cached.list_table_names(schema="public")
    current["now"] = 5
    await cached.list_table_names(schema="public")
    current["now"] = 11
    await cached.list_table_names(schema="public")

    assert wrapped.list_calls == 2


def test_schema_cache_manual_invalidation():
    """Manual invalidation clears cached entries."""
    cache = SchemaCache(ttl_seconds=60)
    key_a = ("postgres", "schema", "public", None, "list_table_names")
    key_b = ("postgres", "schema", "public", "users", "get_table_def")
    cache.set(key_a, ["users"])
    cache.set(key_b, {"name": "users"})

    cache.clear_table("postgres", "public", "users")
    assert cache.get(key_b) is None
    assert cache.get(key_a) == ["users"]

    cache.clear_schema("postgres", "public")
    assert cache.get(key_a) is None
