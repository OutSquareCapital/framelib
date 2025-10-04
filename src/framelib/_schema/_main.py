from __future__ import annotations

import datetime
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

import narwhals as nw
import polars as pl
import pychain as pc

from ._base import Column, TimeUnit


@dataclass(slots=True)
class Datetime(Column):
    time_unit: TimeUnit = "ns"
    time_zone: str | datetime.timezone | None = None

    @property
    def nw_dtype(self) -> nw.Datetime:
        return nw.Datetime(self.time_unit, self.time_zone)

    @property
    def pl_dtype(self) -> pl.Datetime:
        return pl.Datetime(self.time_unit, self.time_zone)

    @property
    def sql_type(self) -> str:
        return "TIME"


@dataclass(slots=True)
class Decimal(Column):
    precision: int | None = None
    scale: int = 0

    @property
    def nw_dtype(self) -> nw.Decimal:
        return nw.Decimal()

    @property
    def pl_dtype(self) -> pl.Decimal:
        return pl.Decimal(self.precision, self.scale)


class Array(Column):
    _inner: Column
    _shape: int | tuple[int, ...]

    def __init__(self, inner: Column, shape: int | tuple[int, ...]) -> None:
        self._inner = inner
        self._shape = shape
        super().__init__()

    @property
    def nw_dtype(self) -> nw.Array:
        return nw.Array(self._inner.nw_dtype, self._shape)

    @property
    def pl_dtype(self) -> pl.Array:
        return pl.Array(self._inner.pl_dtype, self._shape)


class Struct(Column):
    _fields: pc.Dict[str, Column]

    def __init__(self, fields: Mapping[str, Column]) -> None:
        self._fields = pc.Dict.from_map(fields)
        super().__init__()

    @property
    def pl_dtype(self) -> pl.Struct:
        return pl.Struct(self.fields.map_values(lambda col: col.pl_dtype).unwrap())

    @property
    def nw_dtype(self) -> nw.Struct:
        return nw.Struct(self.fields.map_values(lambda col: col.nw_dtype).unwrap())

    @property
    def fields(self) -> pc.Dict[str, Column]:
        """The fields of this struct."""
        return self._fields


class List(Column):
    _inner: Column

    def __init__(self, inner: Column) -> None:
        self._inner = inner
        super().__init__()

    @property
    def nw_dtype(self) -> nw.List:
        return nw.List(self._inner.nw_dtype)

    @property
    def pl_dtype(self) -> pl.List:
        return pl.List(self._inner.pl_dtype)

    @property
    def inner(self) -> Column:
        """The inner column of this list."""
        return self._inner


class Categorical(Column):
    @property
    def nw_dtype(self) -> nw.Categorical:
        return nw.Categorical()

    @property
    def pl_dtype(self) -> pl.Categorical:
        return pl.Categorical()


class Enum(Column):
    _categories: list[str]

    def __init__(self, categories: Iterable[str]) -> None:
        self._categories = list(categories)
        super().__init__()

    @property
    def nw_dtype(self) -> nw.Enum:
        return nw.Enum(self._categories)

    @property
    def pl_dtype(self) -> pl.Enum:
        return pl.Enum(self._categories)

    @property
    def categories(self) -> list[str]:
        """The categories of this enum."""
        return self._categories
