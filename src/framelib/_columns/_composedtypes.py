from __future__ import annotations

import datetime
import enum
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from inspect import isclass
from typing import TYPE_CHECKING, override

import narwhals as nw
import polars as pl
import pyochain as pc

from ._base import Column, TimeUnit

if TYPE_CHECKING:
    from .._core import Layout


@dataclass(slots=True)
class Datetime(Column):
    time_unit: TimeUnit = "ns"
    time_zone: str | datetime.timezone | None = None

    @property
    @override
    def nw_dtype(self) -> nw.Datetime:
        return nw.Datetime(self.time_unit, self.time_zone)

    @property
    @override
    def pl_dtype(self) -> pl.Datetime:
        return pl.Datetime(self.time_unit, self.time_zone)

    @property
    @override
    def sql_type(self) -> str:
        if self.time_zone is None:
            return "TIMESTAMP"
        return "TIMESTAMP WITH TIME ZONE"


@dataclass(slots=True)
class Decimal(Column):
    precision: int | None = 18
    scale: int = 0

    @property
    @override
    def nw_dtype(self) -> nw.Decimal:
        return nw.Decimal()

    @property
    @override
    def pl_dtype(self) -> pl.Decimal:
        return pl.Decimal(self.precision, self.scale)

    @property
    @override
    def sql_type(self) -> str:
        return f"DECIMAL({self.precision}, {self.scale})"


@dataclass(slots=True)
class Array(Column):
    inner: Column
    shape: int | tuple[int, ...]

    @property
    @override
    def nw_dtype(self) -> nw.Array:
        return nw.Array(self.inner.nw_dtype, self.shape)

    @property
    @override
    def pl_dtype(self) -> pl.Array:
        return pl.Array(self.inner.pl_dtype, self.shape)

    @property
    @override
    def sql_type(self) -> str:
        base: str = self.inner.sql_type
        if isinstance(self.shape, int):
            return f"{base}[{self.shape}]"
        dims = pc.Iter(self.shape).map(lambda d: f"[{d}]").join("")
        return f"{base}{dims}"


@dataclass(slots=True, init=False)
class Struct(Column):
    fields: pc.Dict[str, Column]

    def __init__(
        self,
        fields: Mapping[str, Column] | type[Layout[Column]],
        *,
        primary_key: bool = False,
        unique: bool = False,
        nullable: bool = True,
    ) -> None:
        if isclass(fields):
            self.fields = fields.entries()
        else:
            self.fields = pc.Dict(fields)
        super().__init__(primary_key=primary_key, unique=unique, nullable=nullable)

    @property
    @override
    def pl_dtype(self) -> pl.Struct:
        return (
            self.fields
            .items()
            .iter()
            .map_star(lambda name, col: pl.Field(name, col.pl_dtype))
            .collect()
            .into(pl.Struct)
        )

    @property
    @override
    def nw_dtype(self) -> nw.Struct:
        return (
            self.fields
            .items()
            .iter()
            .map_star(lambda name, col: nw.Field(name, col.nw_dtype))
            .collect()
            .into(nw.Struct)
        )

    @property
    @override
    def sql_type(self) -> str:
        return (
            self.fields
            .items()
            .iter()
            .map_star(lambda name, col: f"{name} {col.sql_type}")
            .into(lambda inner: f"STRUCT({inner.join(', ')})")
        )


@dataclass(slots=True)
class List(Column):
    inner: Column

    @property
    @override
    def nw_dtype(self) -> nw.List:
        return nw.List(self.inner.nw_dtype)

    @property
    @override
    def pl_dtype(self) -> pl.List:
        return pl.List(self.inner.pl_dtype)

    @property
    @override
    def sql_type(self) -> str:
        return f"{self.inner.sql_type}[]"


@dataclass(slots=True)
class Categorical(Column):
    @property
    @override
    def nw_dtype(self) -> nw.Categorical:
        return nw.Categorical()

    @property
    @override
    def pl_dtype(self) -> pl.Categorical:
        return pl.Categorical()

    @property
    @override
    def sql_type(self) -> str:
        """Return the enum type as `VARCHAR`.

        Duckdb does not have a native categorical type, it would be more similar to an ENUM.

        Which lead to the same situation as the `Enum` column type (see `Enum.sql_type` docstring).
        """
        return "VARCHAR"


@dataclass(slots=True, init=False)
class Enum(Column):
    categories: pc.Set[str]

    def __init__(
        self,
        categories: Iterable[str] | type[enum.Enum],
        *,
        primary_key: bool = False,
        unique: bool = False,
        nullable: bool = True,
    ) -> None:
        if isclass(categories):
            categories = (item.value for item in categories)  # pyright: ignore[reportAny]
        self.categories = pc.Set(categories)
        super().__init__(primary_key=primary_key, unique=unique, nullable=nullable)

    @property
    @override
    def nw_dtype(self) -> nw.Enum:
        return nw.Enum(self.categories)

    @property
    @override
    def pl_dtype(self) -> pl.Enum:
        return pl.Enum(self.categories)

    @property
    @override
    def sql_type(self) -> str:
        """Return the enum type as `VARCHAR`.

        DuckDB requires a separate `CREATE TYPE ... AS ENUM (...)` statement to define true ENUM types.

        Since `Column` role is not responsible for handling table/database level logic, we return `VARCHAR` here.
        """
        return "VARCHAR"
