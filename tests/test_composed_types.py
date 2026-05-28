"""Tests for composed column types - focus on conversions and complex behaviors."""

from __future__ import annotations

from enum import Enum as StdEnum

import polars as pl
from pyochain import Dict, Iter, Range

import framelib as fl


def test_decimal_sql_type_generation() -> None:
    """SQL type should be correctly formatted."""
    dec = fl.Decimal(precision=19, scale=4)
    assert dec.sql_type == "DECIMAL(19, 4)"


def test_decimal_pl_dtype_matches_polars() -> None:
    """Polars dtype conversion matches direct polars creation."""
    dec = fl.Decimal(precision=10, scale=2)
    assert dec.pl_dtype.precision == pl.Decimal(10, 2).precision
    assert dec.pl_dtype.scale == pl.Decimal(10, 2).scale


def test_array_1d_vs_multidimensional_sql() -> None:
    """SQL generation differs for 1D vs multidimensional."""
    arr_1d = fl.Array(fl.String(), 10)
    arr_2d = fl.Array(fl.String(), (5, 2))
    assert "[5][2]" in arr_2d.sql_type
    assert arr_1d.sql_type != arr_2d.sql_type


def test_array_high_dimensional_sql() -> None:
    """High-dimensional arrays generate correct SQL."""
    arr = fl.Array(fl.Int32(), (2, 3, 4, 5))
    assert arr.sql_type == "INTEGER[2][3][4][5]"


def test_struct_sql_type_includes_all_fields() -> None:
    """SQL contains all field names and types."""
    struct = fl.Struct({"a": fl.Int32(), "b": fl.String()})
    sql = struct.sql_type
    assert "STRUCT" in sql
    assert "a" in sql
    assert "b" in sql


def test_struct_from_schema_inheritance_sql() -> None:
    """Struct from inherited Schema SQL includes all parent/child fields."""

    class BaseSchema(fl.Schema):
        id: fl.Int64 = fl.Int64()

    class DerivedSchema(BaseSchema):
        name: fl.String = fl.String()

    struct = fl.Struct(DerivedSchema)
    sql = struct.sql_type
    assert "id" in sql
    assert "name" in sql


def test_struct_pl_dtype_field_count() -> None:
    """Polars struct dtype has correct number of fields."""
    struct = fl.Struct({"count": fl.Int32(), "label": fl.String()})
    assert len(struct.pl_dtype.fields) == 2


def test_struct_pl_dtype_field_mapping() -> None:
    """Polars struct dtype correctly maps all field types."""
    struct = fl.Struct({"count": fl.Int32(), "label": fl.String()})
    pl_dtype = struct.pl_dtype
    field_names = Iter(pl_dtype.fields).map(lambda f: f.name).collect()
    assert "count" in field_names
    assert "label" in field_names


def test_list_deeply_nested_sql() -> None:
    """Nested lists generate correct SQL with array brackets."""
    lst = (
        Range(0, 3)
        .iter()
        .fold(
            fl.List(fl.Int32()),
            lambda acc, _: fl.List(acc),
        )
    )
    sql = lst.sql_type
    assert sql.endswith("[][]")


def test_list_of_lists_sql() -> None:
    """List of lists generates correct SQL."""
    lst = fl.List(fl.List(fl.String()))
    assert lst.sql_type == "VARCHAR[][]"


def test_list_of_struct_sql() -> None:
    """List of struct generates correct SQL."""
    struct = fl.Struct({"x": fl.Int32(), "y": fl.Int32()})
    lst = fl.List(struct)
    sql = lst.sql_type
    assert "STRUCT" in sql
    assert "x" in sql


def test_enum_pl_dtype_categories() -> None:
    """Enum from list creates valid polars enum dtype."""
    categories = ["red", "green", "blue"]
    enum_dtype = fl.Enum(categories).categories.iter().into(pl.Enum)

    assert str(enum_dtype).startswith("Enum")


def test_enum_from_python_enum_extracts_values() -> None:
    """Enum from Python Enum extracts values, not names."""

    class Status(StdEnum):
        PENDING = "pending_status"
        ACTIVE = "active_status"

    enum_col = fl.Enum(Status)
    assert "pending_status" in enum_col.categories
    assert "active_status" in enum_col.categories


def test_enum_duplicate_categories_deduplicated() -> None:
    """Enum deduplicates via Set."""
    data = ["a", "b", "a", "c", "b"]
    assert fl.Enum(data).categories.len() == 3


def test_datetime_with_timezone_sql() -> None:
    """Datetime with timezone generates TIMESTAMP WITH TIME ZONE SQL."""
    assert fl.Datetime(time_zone="UTC").sql_type == "TIMESTAMP WITH TIME ZONE"


def test_datetime_without_timezone_sql() -> None:
    """Datetime without timezone generates simple TIMESTAMP SQL."""
    assert fl.Datetime().sql_type == "TIMESTAMP"


def test_datetime_pl_dtype_preserves_timezone() -> None:
    """Polars datetime dtype preserves timezone."""
    dt = fl.Datetime(time_zone="America/New_York")
    assert dt.pl_dtype.time_zone == "America/New_York"


def test_struct_with_enum_sql() -> None:
    """Struct with enum generates correct SQL."""
    cats = fl.Enum(["active", "inactive"])
    sql = Dict.from_kwargs(status=cats, id=fl.Int64()).into(fl.Struct).sql_type
    assert "STRUCT" in sql
    assert "status" in sql


def test_struct_with_datetime_sql() -> None:
    """Struct with datetime generates correct SQL."""
    sql = (
        Dict
        .from_kwargs(created=fl.Datetime(time_zone="UTC"), name=fl.String())
        .into(fl.Struct)
        .sql_type
    )
    assert "STRUCT" in sql
    assert "TIMESTAMP" in sql


def test_list_of_lists_of_struct_sql() -> None:
    """Deeply nested structures generate correct SQL."""
    struct = fl.Struct({"value": fl.Float64()})
    lst_2d = (
        Range(0, 2)
        .iter()
        .fold(
            fl.List(struct),
            lambda acc, _: fl.List(acc),
        )
    )
    sql = lst_2d.sql_type
    assert "STRUCT" in sql
    assert "[][]" in sql
