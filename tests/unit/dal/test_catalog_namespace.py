from dal.catalog import CatalogNamespace


def test_catalog_namespace_bigquery():
    """Format BigQuery qualified names."""
    ns = CatalogNamespace("proj", "dataset", "table")
    assert ns.to_bigquery() == "proj.dataset.table"


def test_catalog_namespace_databricks():
    """Format Databricks qualified names."""
    ns = CatalogNamespace("catalog", "schema", "table")
    assert ns.to_databricks() == "catalog.schema.table"


def test_catalog_namespace_snowflake():
    """Format Snowflake-style qualified names."""
    ns = CatalogNamespace("db", "schema", "table")
    assert ns.to_snowflake() == "db.schema.table"
