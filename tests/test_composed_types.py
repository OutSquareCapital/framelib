"""Tests for composed column types - focus on conversions and complex behaviors."""

from enum import Enum as StdEnum

import polars as pl
import pyochain as pc

import framelib as fl


class TestDecimalConversions:
    """Test Decimal type conversions."""

    def test_decimal_sql_type_generation(self) -> None:
        """SQL type should be correctly formatted."""
        dec = fl.Decimal(precision=19, scale=4)
        assert dec.sql_type == "DECIMAL(19, 4)"

    def test_decimal_pl_dtype_matches_polars(self) -> None:
        """Polars dtype conversion matches direct polars creation."""
        dec = fl.Decimal(precision=10, scale=2)
        assert dec.pl_dtype.precision == pl.Decimal(10, 2).precision
        assert dec.pl_dtype.scale == pl.Decimal(10, 2).scale


class TestArrayConversions:
    """Test Array type conversions."""

    def test_array_1d_vs_multidimensional_sql(self) -> None:
        """SQL generation differs for 1D vs multidimensional."""
        arr_1d = fl.Array(fl.String(), 10)
        arr_2d = fl.Array(fl.String(), (5, 2))
        assert "[5][2]" in arr_2d.sql_type
        assert arr_1d.sql_type != arr_2d.sql_type

    def test_array_high_dimensional_sql(self) -> None:
        """High-dimensional arrays generate correct SQL."""
        arr = fl.Array(fl.Int32(), (2, 3, 4, 5))
        assert arr.sql_type == "INTEGER[2][3][4][5]"


class TestStructConversions:
    """Test Struct type conversions."""

    def test_struct_sql_type_includes_all_fields(self) -> None:
        """SQL contains all field names and types."""
        struct = fl.Struct({"a": fl.Int32(), "b": fl.String()})
        sql = struct.sql_type
        assert "STRUCT" in sql
        assert "a" in sql
        assert "b" in sql

    def test_struct_from_schema_inheritance_sql(self) -> None:
        """Struct from inherited Schema SQL includes all parent/child fields."""

        class BaseSchema(fl.Schema):
            id = fl.Int64()

        class DerivedSchema(BaseSchema):
            name = fl.String()

        struct = fl.Struct(DerivedSchema)
        sql = struct.sql_type
        assert "id" in sql
        assert "name" in sql

    def test_struct_pl_dtype_field_count(self) -> None:
        """Polars struct dtype has correct number of fields."""
        struct = fl.Struct({"count": fl.Int32(), "label": fl.String()})
        assert len(struct.pl_dtype.fields) == 2

    def test_struct_pl_dtype_field_mapping(self) -> None:
        """Polars struct dtype correctly maps all field types."""
        struct = fl.Struct({"count": fl.Int32(), "label": fl.String()})
        pl_dtype = struct.pl_dtype
        field_names = pc.Iter(pl_dtype.fields).map(lambda f: f.name).collect()
        assert "count" in field_names
        assert "label" in field_names


class TestListConversions:
    """Test List type conversions."""

    def test_list_deeply_nested_sql(self) -> None:
        """Nested lists generate correct SQL with array brackets."""
        lst = pc.Iter(range(3)).fold(
            fl.List(fl.Int32()),
            lambda acc, _: fl.List(acc),
        )
        sql = lst.sql_type
        assert sql.endswith("[][]")

    def test_list_of_lists_sql(self) -> None:
        """List of lists generates correct SQL."""
        lst = fl.List(fl.List(fl.String()))
        assert lst.sql_type == "VARCHAR[][]"

    def test_list_of_struct_sql(self) -> None:
        """List of struct generates correct SQL."""
        struct = fl.Struct({"x": fl.Int32(), "y": fl.Int32()})
        lst = fl.List(struct)
        sql = lst.sql_type
        assert "STRUCT" in sql
        assert "x" in sql


class TestEnumConversions:
    """Test Enum type conversions."""

    def test_enum_pl_dtype_categories(self) -> None:
        """Enum from list creates valid polars enum dtype."""
        categories = ["red", "green", "blue"]
        enum_col = fl.Enum(categories)
        pl_dtype = pl.Enum(list(enum_col.categories))
        assert str(pl_dtype).startswith("Enum")

    def test_enum_from_python_enum_extracts_values(self) -> None:
        """Enum from Python Enum extracts values, not names."""

        class Status(StdEnum):
            PENDING = "pending_status"
            ACTIVE = "active_status"

        enum_col = fl.Enum(Status)
        assert "pending_status" in enum_col.categories
        assert "active_status" in enum_col.categories

    def test_enum_duplicate_categories_deduplicated(self) -> None:
        """Enum deduplicates via Set."""
        enum_col = fl.Enum(["a", "b", "a", "c", "b"])
        assert enum_col.categories.length() == 3


class TestDatetimeConversions:
    """Test Datetime type conversions."""

    def test_datetime_with_timezone_sql(self) -> None:
        """Datetime with timezone generates TIMESTAMP WITH TIME ZONE SQL."""
        assert fl.Datetime(time_zone="UTC").sql_type == "TIMESTAMP WITH TIME ZONE"

    def test_datetime_without_timezone_sql(self) -> None:
        """Datetime without timezone generates simple TIMESTAMP SQL."""
        assert fl.Datetime().sql_type == "TIMESTAMP"

    def test_datetime_pl_dtype_preserves_timezone(self) -> None:
        """Polars datetime dtype preserves timezone."""
        dt = fl.Datetime(time_zone="America/New_York")
        assert dt.pl_dtype.time_zone == "America/New_York"


class TestComplexInteractions:
    """Test complex composed type interactions."""

    def test_struct_with_enum_sql(self) -> None:
        """Struct with enum generates correct SQL."""
        struct = fl.Struct(
            {"status": fl.Enum(["active", "inactive"]), "id": fl.Int64()}
        )
        sql = struct.sql_type
        assert "STRUCT" in sql
        assert "status" in sql

    def test_struct_with_datetime_sql(self) -> None:
        """Struct with datetime generates correct SQL."""
        struct = fl.Struct(
            {"created": fl.Datetime(time_zone="UTC"), "name": fl.String()}
        )
        sql = struct.sql_type
        assert "STRUCT" in sql
        assert "TIMESTAMP" in sql

    def test_list_of_lists_of_struct_sql(self) -> None:
        """Deeply nested structures generate correct SQL."""
        struct = fl.Struct({"value": fl.Float64()})
        lst_2d = pc.Iter(range(2)).fold(
            fl.List(struct),
            lambda acc, _: fl.List(acc),
        )
        sql = lst_2d.sql_type
        assert "STRUCT" in sql
        assert "[][]" in sql
