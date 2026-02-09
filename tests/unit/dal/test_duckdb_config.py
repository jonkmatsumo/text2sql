from dal.duckdb import DuckDBConfig


def test_duckdb_config_defaults(monkeypatch):
    """Ensure DuckDB config defaults read-only to False."""
    monkeypatch.delenv("DUCKDB_READ_ONLY", raising=False)
    config = DuckDBConfig.from_env()
    assert config.read_only is True


def test_duckdb_config_read_only(monkeypatch):
    """Ensure DuckDB config parses read-only env var."""
    monkeypatch.setenv("DUCKDB_READ_ONLY", "true")
    config = DuckDBConfig.from_env()
    assert config.read_only is True
