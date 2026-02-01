from dal.capabilities import capabilities_for_provider


def test_postgres_capabilities_defaults():
    """Ensure Postgres defaults include full capability support."""
    caps = capabilities_for_provider("postgres")
    assert caps.supports_arrays is True
    assert caps.supports_json_ops is True
    assert caps.supports_transactions is True
    assert caps.supports_fk_enforcement is True


def test_redshift_capabilities():
    """Ensure Redshift disables unsupported capability flags."""
    caps = capabilities_for_provider("redshift")
    assert caps.supports_arrays is False
    assert caps.supports_json_ops is False
    assert caps.supports_transactions is False
    assert caps.supports_fk_enforcement is False
