from dal.capabilities import capabilities_for_provider


def test_postgres_capabilities_defaults():
    """Ensure Postgres defaults include full capability support."""
    caps = capabilities_for_provider("postgres")
    assert caps.execution_model == "sync"
    assert caps.supports_arrays is True
    assert caps.supports_json_ops is True
    assert caps.supports_transactions is True
    assert caps.supports_fk_enforcement is True


def test_redshift_capabilities():
    """Ensure Redshift disables unsupported capability flags."""
    caps = capabilities_for_provider("redshift")
    assert caps.execution_model == "sync"
    assert caps.supports_arrays is False
    assert caps.supports_json_ops is False
    assert caps.supports_transactions is False
    assert caps.supports_fk_enforcement is False


def test_bigquery_capabilities():
    """Ensure BigQuery reports async execution and cost estimation."""
    caps = capabilities_for_provider("bigquery")
    assert caps.execution_model == "async"
    assert caps.supports_cost_estimation is True


def test_athena_capabilities():
    """Ensure Athena reports async execution."""
    caps = capabilities_for_provider("athena")
    assert caps.execution_model == "async"


def test_databricks_capabilities():
    """Ensure Databricks reports async execution."""
    caps = capabilities_for_provider("databricks")
    assert caps.execution_model == "async"
