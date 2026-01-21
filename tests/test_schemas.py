"""Casts, compatibility, composed types creations."""

import pytest

import framelib as fl


def test_instantiate_schema() -> None:
    """Schema cannot be instantiated directly."""
    with pytest.raises(TypeError):
        _ = fl.Schema()


def test_schema_composite_primary_key() -> None:
    """Verify that a Schema can declare multiple primary_key columns (composite PK)."""

    class S(fl.Schema):
        a = fl.Int64(primary_key=True)
        b = fl.Int64(primary_key=True)
        val = fl.String()

    constraints = S.constraints()
    assert constraints.primary.is_some()
    pk_cols = constraints.primary.unwrap().cols
    assert pk_cols.length() == 2
    sql_col = constraints.primary.unwrap().to_sql()
    assert "a" in sql_col
    assert "b" in sql_col


def test_schema_composite_pk_sql_generation() -> None:
    """Composite PK should generate table-level PRIMARY KEY constraint in SQL."""

    class S(fl.Schema):
        a = fl.Int64(primary_key=True)
        b = fl.Int64(primary_key=True)
        val = fl.String()

    sql = S.to_sql()
    # Columns should NOT have PRIMARY KEY individually
    assert sql.count("PRIMARY KEY") == 1
    # Should have table-level constraint
    assert 'PRIMARY KEY ("a", "b")' in sql or 'PRIMARY KEY ("b", "a")' in sql


def test_schema_single_pk_sql_generation() -> None:
    """Single PK should generate column-level PRIMARY KEY constraint in SQL."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)
        name = fl.String()

    sql = S.to_sql()
    # Should have column-level PRIMARY KEY
    assert '"id" BIGINT PRIMARY KEY' in sql
    # Should NOT have table-level constraint
    assert "PRIMARY KEY (" not in sql


def test_schema_empty_allowed() -> None:
    """Empty Schema is allowed."""

    class EmptyS(fl.Schema):
        pass

    assert EmptyS.entries().length() == 0


def test_schema_inheritance_column_order_preservation() -> None:
    """Inherited schema preserves parent columns before child columns."""

    class ParentS(fl.Schema):
        parent_col1 = fl.Int64()
        parent_col2 = fl.String()

    class ChildS(ParentS):
        child_col = fl.Float64()

    col_names = ChildS.entries().keys().into(tuple)
    # Parent columns should come first
    assert col_names.index("parent_col1") < col_names.index("child_col")
    assert col_names.index("parent_col2") < col_names.index("child_col")


def test_schema_to_sql_simple() -> None:
    """Schema generates valid SQL for simple structure."""

    class SimpleS(fl.Schema):
        id = fl.Int64()
        name = fl.String()

    sql = SimpleS.to_sql()
    assert '"id"' in sql
    assert '"name"' in sql


def test_schema_unique_constraint_sql_generation() -> None:
    """Unique constraint should generate UNIQUE in column SQL."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)
        email = fl.String(unique=True)
        name = fl.String()

    sql = S.to_sql()
    assert '"email" VARCHAR UNIQUE' in sql
    # Verify constraints() tracks unique columns
    constraints = S.constraints()
    assert constraints.uniques.is_some()
    assert "email" in constraints.uniques.unwrap().to_sql()


def test_schema_unique_and_pk_combined() -> None:
    """Column can have both primary_key and unique (though redundant)."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True, unique=True)
        name = fl.String()

    sql = S.to_sql()
    assert "PRIMARY KEY" in sql
    assert "UNIQUE" in sql


def test_schema_not_null_constraint_sql_generation() -> None:
    """NOT NULL constraint should generate NOT NULL in column SQL."""

    class S(fl.Schema):
        id = fl.Int64(primary_key=True)
        name = fl.String(nullable=False)
        description = fl.String()  # nullable by default

    sql = S.to_sql()
    assert '"name" VARCHAR NOT NULL' in sql
    assert '"description" VARCHAR' in sql
    assert "description" not in sql.replace('"description" VARCHAR', "")  # no NOT NULL
    # Verify constraints() tracks not_null columns
    constraints = S.constraints()
    assert constraints.not_nulls.is_some()
    assert "name" in constraints.not_nulls.unwrap().to_sql()


def test_schema_composite_unique_sql_generation() -> None:
    """Multiple unique columns should generate table-level UNIQUE constraint."""

    class S(fl.Schema):
        a = fl.Int64(unique=True)
        b = fl.Int64(unique=True)
        val = fl.String()

    sql = S.to_sql()
    # Should NOT have column-level UNIQUE
    assert '"a" BIGINT UNIQUE' not in sql
    assert '"b" BIGINT UNIQUE' not in sql
    # Should have table-level UNIQUE constraint
    assert "UNIQUE (" in sql
    assert sql.count("UNIQUE") == 1
