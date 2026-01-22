"""Unit tests for schema models."""

from schema import ColumnDef, ForeignKeyDef, TableDef


def test_column_def_construction():
    """Test ColumnDef basic construction."""
    col = ColumnDef(name="id", data_type="integer", is_nullable=False)
    assert col.name == "id"
    assert col.data_type == "integer"
    assert col.is_nullable is False
    assert col.is_primary_key is False
    assert col.description is None


def test_column_def_with_all_fields():
    """Test ColumnDef with all optional fields."""
    col = ColumnDef(
        name="user_id",
        data_type="int",
        is_nullable=True,
        is_primary_key=True,
        description="Primary key",
    )
    assert col.is_primary_key is True
    assert col.description == "Primary key"


def test_foreign_key_def_construction():
    """Test ForeignKeyDef construction."""
    fk = ForeignKeyDef(
        column_name="user_id",
        foreign_table_name="users",
        foreign_column_name="id",
    )
    assert fk.column_name == "user_id"
    assert fk.foreign_table_name == "users"
    assert fk.foreign_column_name == "id"


def test_table_def_construction():
    """Test TableDef basic construction."""
    table = TableDef(name="orders")
    assert table.name == "orders"
    assert table.columns == []
    assert table.foreign_keys == []
    assert table.description is None
    assert table.sample_data == []


def test_table_def_with_columns():
    """Test TableDef with column definitions."""
    cols = [
        ColumnDef(name="id", data_type="int", is_nullable=False, is_primary_key=True),
        ColumnDef(name="status", data_type="text", is_nullable=True),
    ]
    table = TableDef(name="orders", columns=cols, description="Order table")
    assert len(table.columns) == 2
    assert table.columns[0].name == "id"
    assert table.description == "Order table"


def test_table_def_with_foreign_keys():
    """Test TableDef with foreign key definitions."""
    fk = ForeignKeyDef(
        column_name="user_id",
        foreign_table_name="users",
        foreign_column_name="id",
    )
    table = TableDef(name="orders", foreign_keys=[fk])
    assert len(table.foreign_keys) == 1
    assert table.foreign_keys[0].foreign_table_name == "users"


def test_models_are_mutable():
    """Test that models are mutable (frozen=False)."""
    col = ColumnDef(name="id", data_type="int", is_nullable=False)
    col.name = "new_name"
    assert col.name == "new_name"

    table = TableDef(name="test")
    table.description = "Updated"
    assert table.description == "Updated"
