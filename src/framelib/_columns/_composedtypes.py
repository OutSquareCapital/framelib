import datetime
import enum
from collections.abc import Iterable, Mapping
from inspect import isclass
from typing import TYPE_CHECKING

import narwhals as nw
import polars as pl
import pyochain as pc

from ._base import Column, TimeUnit

if TYPE_CHECKING:
    from .._core import Layout


class Datetime(Column):
    time_unit: TimeUnit
    time_zone: str | datetime.timezone | None

    def __init__(
        self,
        time_unit: TimeUnit = "ns",
        time_zone: str | datetime.timezone | None = None,
        *,
        primary_key: bool = False,
        unique: bool = False,
    ) -> None:
        self.time_unit = time_unit
        self.time_zone = time_zone
        super().__init__(primary_key=primary_key, unique=unique)

    @property
    def nw_dtype(self) -> nw.Datetime:
        return nw.Datetime(self.time_unit, self.time_zone)

    @property
    def pl_dtype(self) -> pl.Datetime:
        return pl.Datetime(self.time_unit, self.time_zone)

    @property
    def sql_type(self) -> str:
        if self.time_zone is None:
            return "TIMESTAMP"
        return "TIMESTAMP WITH TIME ZONE"


class Decimal(Column):
    precision: int | None
    scale: int

    def __init__(
        self,
        precision: int | None = None,
        scale: int = 0,
        *,
        primary_key: bool = False,
        unique: bool = False,
    ) -> None:
        self.precision = precision
        self.scale = scale
        super().__init__(primary_key=primary_key, unique=unique)

    @property
    def nw_dtype(self) -> nw.Decimal:
        return nw.Decimal()

    @property
    def pl_dtype(self) -> pl.Decimal:
        return pl.Decimal(self.precision, self.scale)

    @property
    def sql_type(self) -> str:
        if self.precision is None:
            return "DECIMAL"
        return f"DECIMAL({self.precision}, {self.scale})"


class Array(Column):
    _inner: Column
    _shape: int | tuple[int, ...]

    def __init__(
        self,
        inner: Column,
        shape: int | tuple[int, ...],
        *,
        primary_key: bool = False,
        unique: bool = False,
    ) -> None:
        self._inner = inner
        self._shape = shape
        super().__init__(primary_key=primary_key, unique=unique)

    @property
    def nw_dtype(self) -> nw.Array:
        return nw.Array(self._inner.nw_dtype, self._shape)

    @property
    def pl_dtype(self) -> pl.Array:
        return pl.Array(self._inner.pl_dtype, self._shape)

    @property
    def sql_type(self) -> str:
        base: str = self._inner.sql_type
        if isinstance(self._shape, int):
            return f"{base}[{self._shape}]"
        dims = "".join(f"[{d}]" for d in self._shape)
        return f"{base}{dims}"

    @property
    def inner(self) -> Column:
        """Get the inner column of this array."""
        return self._inner

    @property
    def shape(self) -> int | tuple[int, ...]:
        """Get the shape of this array."""
        return self._shape


class Struct(Column):
    _fields: pc.Dict[str, Column]

    def __init__(
        self,
        fields: Mapping[str, Column] | type[Layout[Column]],
        *,
        primary_key: bool = False,
        unique: bool = False,
    ) -> None:
        if isclass(fields):
            self._fields = fields.schema()
        else:
            self._fields = pc.Dict.from_(fields)
        super().__init__(primary_key=primary_key, unique=unique)

    @property
    def pl_dtype(self) -> pl.Struct:
        return pl.Struct(self.fields.map_values(lambda col: col.pl_dtype).inner())

    @property
    def nw_dtype(self) -> nw.Struct:
        return nw.Struct(self.fields.map_values(lambda col: col.nw_dtype).inner())

    @property
    def sql_type(self) -> str:
        def format_item(name: str, t: str) -> str:
            return f"{name} {t}"

        inner = (
            self.fields.map_values(lambda col: col.sql_type)
            .iter_items()
            .map(lambda it: format_item(*it))
            .join(", ")
        )
        return f"STRUCT({inner})"

    @property
    def fields(self) -> pc.Dict[str, Column]:
        """Get the fields of this struct."""
        return self._fields


class List(Column):
    _inner: Column

    def __init__(
        self,
        inner: Column,
        *,
        primary_key: bool = False,
        unique: bool = False,
    ) -> None:
        self._inner = inner
        super().__init__(primary_key=primary_key, unique=unique)

    @property
    def nw_dtype(self) -> nw.List:
        return nw.List(self._inner.nw_dtype)

    @property
    def pl_dtype(self) -> pl.List:
        return pl.List(self._inner.pl_dtype)

    @property
    def sql_type(self) -> str:
        return f"{self._inner.sql_type}[]"

    @property
    def inner(self) -> Column:
        """Get the inner column of this list."""
        return self._inner


class Categorical(Column):
    @property
    def nw_dtype(self) -> nw.Categorical:
        return nw.Categorical()

    @property
    def pl_dtype(self) -> pl.Categorical:
        return pl.Categorical()

    @property
    def sql_type(self) -> str:
        """Return the enum type as `VARCHAR`.

        Duckdb does not have a native categorical type, it would be more similar to an ENUM.

        Which lead to the same situation as the `Enum` column type (see `Enum.sql_type` docstring).
        """
        return "VARCHAR"


class Enum(Column):
    _categories: list[str]

    def __init__(
        self,
        categories: Iterable[str] | type[enum.Enum],
        *,
        primary_key: bool = False,
        unique: bool = False,
    ) -> None:
        if isclass(categories):
            categories = (item.value for item in categories)
        self._categories = list(categories)
        super().__init__(primary_key=primary_key, unique=unique)

    @property
    def nw_dtype(self) -> nw.Enum:
        return nw.Enum(self._categories)

    @property
    def pl_dtype(self) -> pl.Enum:
        return pl.Enum(self._categories)

    @property
    def sql_type(self) -> str:
        """Return the enum type as `VARCHAR`.

        DuckDB requires a separate `CREATE TYPE ... AS ENUM (...)` statement to define true ENUM types.

        Since `Column` role is not responsible for handling table/database level logic, we return `VARCHAR` here.
        """
        return "VARCHAR"

    @property
    def categories(self) -> list[str]:
        """Get the categories of this enum as a `list[str]`."""
        return self._categories
